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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

object FixedThreshold {

  //Logger
  val rootLoggerLevel = Level.WARN


  def monitorTagForFixedThreshold(spanStream: DStream[(Host, Span)], tagToMonitor: String, threshold: Double, tagShouldAlwaysBeAvailable: Boolean, anomalyOutputTopic: String, kafkaServers: String, printAnomaly: Boolean, writeToKafka: Boolean) = {

    val valueStream = spanStream.map(x => (x._1, x._2, x._2.tags().get(tagToMonitor)))

    //filter out all spans where the tag was not available (get returning null) if the tag is not defined as always available

    val cleanedValueStream = valueStream.filter(x => {
      if (!tagShouldAlwaysBeAvailable) {
        x._3 != null
      } else {
        true
      }
    })

    cleanedValueStream.foreachRDD(rdd => {

      rdd.foreach(x => {
        try{
        val value = x._3.toDouble
        if (value > threshold) {
          
          
            reportAnomaly(value, threshold, tagToMonitor, x._2, x._1,anomalyOutputTopic, kafkaServers, printAnomaly, writeToKafka)
        
          
        }
        }catch{
          case numberFormatException: java.lang.NumberFormatException => {println("NumberFormatException while trying to convert value of tag "+tagToMonitor)}
        }
      })

      //      if(writeToKafka){
      //        printErrorsAndWriteToTopic(rdd, defaultErrorOutputTopic)
      //      }else{
      //        printErrors(rdd)
      //      }

    })

    Logger.getRootLogger.setLevel(rootLoggerLevel)
  }

  private def reportAnomaly(value: Double, threshold: Double, tagToMonitor:String, span: Span, host: Host, anomalyOutputTopic: String, kafkaServers: String, printAnomaly: Boolean, writeToKafka: Boolean) = {

    val anomalyDescriptor = "fixedThreshold-"+tagToMonitor
    
    val anomalyJSON = StreamUtil.generateAnomalyJSON(host, span, anomalyDescriptor)
      
    if(printAnomaly){
      println(anomalyJSON)
    }
    
    if(writeToKafka){
      val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    val message = new ProducerRecord[String, String](anomalyOutputTopic, null, anomalyJSON)
    producer.send(message)
    }
    

  }

}

