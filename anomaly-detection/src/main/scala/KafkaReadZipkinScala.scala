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

object KafkaReadZipkinScala {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("KafkaWordCountScala").setMaster("local[4]").set("spark.local.dir", "C:/tmp")

    sparkConf.registerKryoClasses(Array(classOf[Span]))
    sparkConf.registerKryoClasses(Array(classOf[Spans]))
    sparkConf.registerKryoClasses(Array(classOf[java.util.Map[String, String]]))
    
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "zipkinreader",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("sleuth")
    
    
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
     val spanStream = spansStream.flatMap { x => x.getSpans.asScala }
//     spanStream.print(100);
    
    val spanTagStream = spanStream.map( x =>  x.tags().asScala.filter(x => x._1.equals("customTag")))
//    spanTagStream.print(1000)
    
    //Stream of Tuples of all Spans and the services there Spans were associated with
     val spanServiceNameStream = spansStream.flatMap( x => x.getSpans.asScala.map(y => (x.getHost.getServiceName, y.getName()))).groupByKey()
//     spanServiceNameStream.print(20)
    
     //stream to count the number of Spans each Spans-Object has associated with
     val spansCountStream = spansStream.map { x => (x.getHost.getServiceName, x.getSpans.size()) }
//     spansCountStream.print(100)

    //Stream of Span-Names and an Iterator of all the Spans with the same name
     val spanNameStream = spanStream.map(x => (x.getName, x)).groupByKey()
//     spanNameStream.print(25)
//     spanNameStream.count().print()
     
     val spanNameDurationStream = spanStream.map(x => (x.getName, x.getAccumulatedMicros)).groupByKey()
//     spanNameDurationStream.print(25)

     
     //Stream with (#,min,max,avg) for Duration of Spans groupedBy their name
     val spanNameDurationStatisticsStream = spanNameDurationStream.map(x => (x._1, (x._2.size, x._2.min, x._2.max, x._2.reduce((a,b) => (a+b)/2))))
     spanNameDurationStatisticsStream.print(50)
         
         
     //groups the stream of span into buckets by TraceId
     val  spanTraceStream = spanStream.map { x => (Span.idToHex(x.getTraceId), x) }.groupByKeyAndWindow(Seconds(60))
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



    Logger.getRootLogger.setLevel(Level.WARN)
    ssc.start()
    ssc.awaitTermination()
  }

}

