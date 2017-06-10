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
    
//    json.print()
    
//    val parsedJSON = json.map { x => JSON.parseFull(x) }
//    
//    val parsedSpans = parsedJSON.flatMap { x => x.get.asInstanceOf[Map[String,Any]].get("spans").get.asInstanceOf[List[Map[String,Any]]] }
//    .asInstanceOf[List[Map[String, Any]]]
    
    
    val spansStream = json.map(x =>
      {
        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.readValue[Spans](x, classOf[Spans])

        
      })
    
//    val spans = parsedSpans.map(x => Span.convertToSpan(x))
    val spanStream = spansStream.flatMap { x => x.getSpans.asScala }
      
    val  spanTupleStream = spanStream.map { x => (Span.idToHex(x.getTraceId), x) }.groupByKeyAndWindow(Seconds(60))
    
    spanTupleStream.print()

//    
//    val singleSpans = spans.flatMap { x => x.getSpans.asScala }
//    
//    spans.print()
    


    Logger.getRootLogger.setLevel(Level.WARN)
    ssc.start()
    ssc.awaitTermination()
  }

}

