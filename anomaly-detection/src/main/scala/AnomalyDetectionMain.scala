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
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.OffsetRange
import kafka.tools.UpdateOffsetsInZK
import org.apache.spark.streaming.kafka010.KafkaRDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.State

object AnomalyDetectionMain {
  //General
  val whitelistedStatusCodes = Array("200", "201")
  val spanNameFilter = "http:/distance"
  //  val spanNameFilter ="http:/login"

  val spanNameFilterSet = Set("http:/business-core-service/businesses/list", "http:/accounting-core-service/drive-now/1/book")

  //kMeans
  val k = 1
  var flag = 0

  //Logger
  val rootLoggerLevel = Level.WARN

  //Kafka
  val kafkaServers = "localhost:9092"
  val sleuthInputTopic = "sleuth"
  val errorOutputTopic = "error"
  //  val kafkaAutoOffsetReset = "earliest" //use "earliest" to read as much from queue as possible or "latest" to only get new values

  //Spark
  val sparkAppName = "KafkaKMeans"
  val sparkMaster = "local[4]"
  val sparkLocalDir = "C:/tmp"
  val batchInterval = 5
  val checkpoint = "checkpoint"

  //global fields
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
      .set("spark.local.dir", sparkLocalDir)
      .set("spark.driver.allowMultipleContexts", "true")

    sparkConf.registerKryoClasses(Array(classOf[Span]))
    sparkConf.registerKryoClasses(Array(classOf[Spans]))
    sparkConf.registerKryoClasses(Array(classOf[java.util.Map[String, String]]))

    val trainingSsc = new StreamingContext(sparkConf, Seconds(batchInterval))
    val sparkContext = trainingSsc.sparkContext
    

    trainingSsc.checkpoint(checkpoint)
    
    val trainingSpanStream = getSpanStreamFromKafka(trainingSsc, "earliest", "training")
    
    val trainingHttpSpanStream = filterSpanStreamForHttpRequests(trainingSpanStream)
    
    val result = KafkaSplittedKMeans.trainKMeansOnInitialSpanStream(trainingSsc, trainingHttpSpanStream)
    
    trainingSsc.stop(false,false)
    println("SSC stopped")
    
    for(resultLine <- result){
      printResultLine(resultLine)
    }
    
    val predictionSsc = new StreamingContext(sparkContext, Seconds(batchInterval))
    predictionSsc.checkpoint("pred-checkpoint")

    val models = result.toMap
    
    val predictionSpanStream = getSpanStreamFromKafka(predictionSsc, "latest", "prediction")

    val predictionHttpSpanStream = filterSpanStreamForHttpRequests(predictionSpanStream)
    
    ErrorDetection.errorDetection(predictionSpanStream, false) //do not write to Kafka - just print errors
    
    FixedThreshold.monitorTagForFixedThreshold(predictionSpanStream, "cpu.system.utilization", 100, true, false)
    
    FixedThreshold.monitorTagForFixedThreshold(predictionSpanStream, "jvm.memoryUtilization", 85, true, false)

    
    
    KafkaSplittedKMeans.kMeansAnomalyDetection(predictionSsc, predictionHttpSpanStream, models)

    predictionSsc.start()
    predictionSsc.awaitTermination()
  }
  
  def filterSpanStreamForHttpRequests(spanStream: DStream[(Host, Span)]): DStream[(Host, Span)]= {
    spanStream.filter(x => x._2.tags().keySet().contains("http.method")||x._2.getSpanId==x._2.getTraceId)
  }
  
  def printResultLine(resultLine: (String,(KMeansModel, Double, Double, Double, Double)))={
    
    val result = resultLine._2
    val spanName = resultLine._1
    
    val model = result._1
    val percentile99 = Math.sqrt(result._2)
    val median = Math.sqrt(result._3)
    val avg = Math.sqrt(result._4)
    val max = Math.sqrt(result._5)

    println("----Results for "+ spanName +"----")
    for (i <- 0 until model.clusterCenters.length) {
      println("Centroid: " + model.clusterCenters(i))
    }
    println("----Distances----")
    println("99 percentile: " + percentile99)
    println("Median: " + median)
    println("Average: " + avg)
    println("Max: " + max)
  }
  
  /**
   * autoOffsetReset can either be:
   * "earliest": Reads from the earliest possible value in kafka topic
   * "latest": Reads only the most recent values from kafka topic
   *
   */
  //TODO: Think about making autoOffsetReset an enum
  def getSpanStreamFromKafka(ssc: StreamingContext, autoOffsetReset: String, groupId: String): DStream[(Host, Span)] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(sleuthInputTopic)
    

    val sleuthWithHeader = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value())
    
    //    Would be a better way if it was not necessary to create a stream
    //    val sleuthBatch = KafkaUtils.createRDD(ssc.sparkContext, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent)

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
    val serviceStream = spansStream.map(x => (x.getHost.getServiceName, x.getSpans.asScala)).groupByKey()
    //      serviceStream.print(25)

    //Stream of all Spans that come from Kafka
    val spanStream = spansStream.flatMap(x => {
      var buffer = ArrayBuffer[(Host, Span)]()
      x.getSpans.asScala.foreach(y => buffer.append((x.getHost, y)))
      buffer
    })

    
//    val spec = StateSpec.function(mappingFunction _)
//    val preparedSpanStream = spanStream.map(x => ("readNames", x))
//    
//    val stateSpanStream = preparedSpanStream.mapWithState(spec)
    
    
    
    spanStream

  }


}

