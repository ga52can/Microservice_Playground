import org.apache.log4j.{ Level, Logger }
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.Minutes
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import collection.JavaConverters._
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.parsing.json.JSON
import org.springframework.cloud.sleuth.Span
import org.springframework.cloud.sleuth.stream.Spans
import org.springframework.cloud.sleuth.stream.Host
import scala.collection.mutable.ArrayBuffer
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.HashMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

object KafkaStreamingKMeans {
  //General
  val whitelistedStatusCodes = Array("200", "201")
  val spanNameFilter ="http:/business-core-service/businesses/list"
  
  //Logger
  val rootLoggerLevel = Level.WARN
  
  //Kafka
  val kafkaServers = "localhost:9092"
  val sleuthInputTopic = "sleuth"
  val errorOutputTopic = "error"
  val kafkaAutoOffsetReset = "earliest" //use "earliest" to read as much from queue as possible or "latest" to only get new values
  
  //Spark
  val sparkAppName = "KafkaWordCountScala"
  val sparkMaster = "local[4]"
  val sparkLocalDir = "C:/tmp"
  val batchInterval = 5
  val checkpoint = "checkpoint"
  
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(rootLoggerLevel)

    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster).set("spark.local.dir", sparkLocalDir)

    sparkConf.registerKryoClasses(Array(classOf[Span]))
    sparkConf.registerKryoClasses(Array(classOf[Spans]))
    sparkConf.registerKryoClasses(Array(classOf[java.util.Map[String, String]]))
    
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))
    ssc.checkpoint(checkpoint)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streamingkmeans",
      "auto.offset.reset" -> kafkaAutoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(sleuthInputTopic)
    
    
    val sleuthWithHeader = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value())

    
    val json = sleuthWithHeader.map(x => x.substring(x.indexOf("{\"host\":")))
//    json.print(1000)
    
    
    val spansStream = json.map(x =>
      {
        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.readValue[Spans](x, classOf[Spans])

        
      })

      //Stream that maps the each Spans object as a Tuple of ServiceName and a List of its associated Spans
      val serviceStream = spansStream.map(x=> (x.getHost.getServiceName, x.getSpans.asScala)).groupByKey()
//      serviceStream.print(25)

      
     //Stream of all Spans that come from Kafka
     val spanStream = spansStream.flatMap ( x => {
       var buffer =  ArrayBuffer[(Host, Span)]()
       x.getSpans.asScala.foreach( y => buffer.append((x.getHost, y)))
       buffer })
      
     val filteredSpanStream = spanStream.filter { x => x._2.getName.equals(spanNameFilter) }  
       
     val spanDurationVectorStream = filteredSpanStream.map(x => Vectors.dense(x._2.getAccumulatedMicros))
     val spanLabledDurationVectorStream = spanStream.map(x => (x._2.getSpanId,Vectors.dense(x._2.getAccumulatedMicros)))
     
     val model = new StreamingKMeans()
                 .setK(2)
                 .setDecayFactor(1.0)
//                 .setInitialCenters(Array[Vector](Vectors.dense(5000.0), Vectors.dense(10000000.0)), Array(0.0,0.0))
                 .setRandomCenters(1, 0.0)
    
       model.trainOn(spanDurationVectorStream)
       model.predictOnValues(spanLabledDurationVectorStream)
       .map(x => {
         var result : (Long, Int)= x
         model.latestModel().clusterCenters.foreach(println)
         result
       }) 
       .print(25)
       
       
       
     


    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()
    ssc.awaitTermination()
  }

}

