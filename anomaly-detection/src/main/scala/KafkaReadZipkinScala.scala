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

object KafkaReadZipkinScala {
  //General
  val whitelistedStatusCodes = Array("200", "201")
  
  //Logger
  val rootLoggerLevel = Level.WARN
  
  //Kafka
  val kafkaServers = "localhost:9092"
  val sleuthInputTopic = "sleuth"
  val errorOutputTopic = "error"
  val kafkaAutoOffsetReset = "earliest" //use "earliest" to read as much from queue as possible or "latest" to only get new values
  
  //Spark
  val sparkAppName = "KafkaReadZipkin"
  val sparkMaster = "local[4]"
  val sparkLocalDir = "C:/tmp"
  val batchInterval = 5
  val checkpoint = "checkpoint-read"
  
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
      "group.id" -> "zipkinreader",
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
       
     
//     spanStream.count().print(50);
//     spanStream.count().print();
    
    val spanTagStream = spanStream.map( x =>  (x._1, x._2, x._2.tags().asScala.map(y => y))) //map required to prevent some weird serialization issue
//    spanTagStream.print(1000)
    
    //returns a map of tags of all spans that have the tag status_code set and which have not the value 200 or 201
    val spanErrorStream = spanTagStream.filter(x => {
      val status_code = x._3.get("http.status_code").getOrElse("").asInstanceOf[String]
      if (status_code.isEmpty() || whitelistedStatusCodes.contains(status_code)) false
      else true
       
    })
    
//    spanErrorStream.print(100)
    
    
    
    //Stream of Tuples of all Spans and the services there Spans were associated with
     val spanServiceNameStream = spansStream.flatMap( x => x.getSpans.asScala.map(y => (x.getHost.getServiceName, y.getName()))).groupByKey()
//     spanServiceNameStream.print(20)
    
     //stream to count the number of Spans each Spans-Object has associated with
     val spansCountStream = spansStream.map { x => (x.getHost.getServiceName, x.getSpans.size()) }
//     spansCountStream.print(100)

    //Stream of Span-Names and an Iterator of all the Spans with the same name
     val spanNameStream = spanStream.map(x => (x._2.getName, x)).groupByKey()
//     spanNameStream.print(25)
//     spanNameStream.count().print()
     
     val spanNameDurationStream = spanStream.map(x => (x._2.getName, x._2.getAccumulatedMicros)).groupByKey()
//     spanNameDurationStream.print(25)

     
     //Stream with (#,min,max,avg) for Duration of Spans groupedBy their name
     val spanNameDurationStatisticsStream = spanNameDurationStream.map(x => (x._1, (x._2.size, x._2.min, x._2.max, x._2.reduce((a,b) => (a+b)/2))))
     spanNameDurationStatisticsStream.print(50)
         
         
     //groups the stream of span into buckets by TraceId
     val  spanTraceStream = spanStream.map { x => (Span.idToHex(x._2.getTraceId), x._2) }.groupByKeyAndWindow(Seconds(60))
//     spanTraceStream.print(50) 
//     spanTraceStream.count().print()
        
     
     // Stream that counts the Spans associated to each traceId and returns a Tuple TraceId/Count
     val  spanTraceCountStream = spanTraceStream.map(x=> (x._1, x._2.size))
//     spanTraceCountStream.print(100)
     
     
    //Stream of Tuples of TraceIds/Span for all TraceIds from spanTraceStream with the corresponding rootSpan(with max AccumulatedMicros grouped - parent always null in tested data)
     val spanTraceStreamMaxDurationSpan = spanTraceStream.map(x => (x._1, x._2.reduce((a, b) => if(a.getAccumulatedMicros>b.getAccumulatedMicros) a else b)))
//     spanTraceStreamMaxDurationSpan.print(50)
     
     
     //Stream that shows the associated log-events of the Span with the longest duration of a certain Trace
     val spanTraceStreamMaxDurationSpanLogs = spanTraceStreamMaxDurationSpan.map(x => (x._1, x._2.logs().asScala.map { x => x.getEvent }))
//     spanTraceStreamMaxDurationSpanLogs.print(50)

     
     
    spanErrorStream.foreachRDD(rdd =>
      rdd.foreachPartition(
          partitionOfRecords =>
            {
                val props = new HashMap[String, Object]()
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer")
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer")
                val producer = new KafkaProducer[String,String](props)

                partitionOfRecords.foreach
                {
                     x=>{
                        
                        
                        val host =   x._1
                        val span =   x._2
                        val tagMap = x._3
                        
                        val hostString = host.getServiceName + "@" + host.getAddress + ":" + host.getPort
                        val spanName= span.getName
                        val traceId = span.getTraceId
                        val httpStatusCode = tagMap.getOrElse("http.status_code", "").asInstanceOf[String];
                        val errorMessage =tagMap.getOrElse("error", "")
                        val errorJSON = "{\"host\":\""+ hostString + "\", \"spanName\":\""+spanName+"\", \"traceId\":\"" +traceId+"\", \"status_code\":\"" + httpStatusCode+ "\", \"errorMessage\":\""+errorMessage +"\"}"

                        
                        val message=new ProducerRecord[String, String](errorOutputTopic,null,errorJSON)
                        producer.send(message)
                        println(errorJSON)
                    }
                }
          })
) 

    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()
    ssc.awaitTermination()
  }

}

