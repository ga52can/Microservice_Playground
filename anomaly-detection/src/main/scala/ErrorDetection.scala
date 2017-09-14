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

object ErrorDetection {

  //Logger
  val rootLoggerLevel = Level.WARN

  //General
  val whitelistedStatusCodes = Array("200", "201")

  def errorDetection(spanStream: DStream[(Host, Span)], anomalyOutputTopic: String, kafkaServers: String, printAnomaly: Boolean, writeToKafka: Boolean) = {

    val spanTagStream = spanStream.map(x => (x._1, x._2, x._2.tags().asScala.map(y => y))) //map required to prevent some weird serialization issue

    //returns a map of tags of all spans that have the tag status_code set and which have not the value 200 or 201
    val spanErrorStream = spanTagStream.filter(x => errorFilter(x))

    spanErrorStream.foreachRDD(rdd => {
      if (writeToKafka) {
        reportAnomaly(rdd, anomalyOutputTopic, kafkaServers, printAnomaly, writeToKafka)
      } else {
        printErrors(rdd)
      }

    })

    Logger.getRootLogger.setLevel(rootLoggerLevel)
  }

  /**
   * This filter is specifically designed for the error reporting behaviour of
   * spring cloud sleuth
   */
  private def errorFilter(input: (Host, Span, Map[String, String])) = {
    val status_code = input._3.get("http.status_code").getOrElse("").asInstanceOf[String]
    //filter out every Span that has a white listed status code and no error tag set
    if ((status_code.isEmpty() || whitelistedStatusCodes.contains(status_code)) && input._2.tags().get("error") == null) 
      false
    //A span with status code 404 is (at least in this setup) only issued in the case of a call of a not existing page
    //This means it should usually only be a problem that is caused by a user entering a wrong url
    //If (in this demo setup) an internally called service is not available a 500 + error message is issued
    else if(status_code.equals("404"))
        false
    //only return the 500er spans that actually do have an error message - the error message is set to the first span
    //where it appeared, while the whole stack back to the root will also have 500er spans but without error message
    //by removing all 500er spans without error set we get rid of those
    else if(status_code.equals("500") && input._3.getOrElse("error", "not set").equals("not set"))
      false
      
    //only return the 400er spans that actually do have an error message - the error message is set to the first span where the error happened
    else if(status_code.equals("400") && input._3.getOrElse("error", "not set").equals("not set"))
      false
    
    else true
  }

  private def reportAnomaly(rdd: RDD[(Host, Span, Map[String, String])], errorOutputTopic: String, kafkaServers: String, printAnomaly: Boolean, writeToKafka: Boolean) = {
    rdd.foreachPartition(
      partitionOfRecords =>
        {

          val props = new HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](props)

          partitionOfRecords.foreach {
            x =>
              {

                val host = x._1
                val span = x._2
                val tagMap = x._3
                val httpStatusCode = tagMap.getOrElse("http.status_code", "").asInstanceOf[String];
                val errorMessage = tagMap.getOrElse("error", "not set")

                val errorCode = "Error: " + httpStatusCode +" - Message: "+errorMessage
                val anomalyJson = StreamUtil.generateAnomalyJSON(host, span, errorCode)
                val message = new ProducerRecord[String, String](errorOutputTopic, null, anomalyJson)
                if (writeToKafka) {
                  producer.send(message)
                }
                if (printAnomaly) {
                  println("Error: " + anomalyJson)
                }
              }
          }
          producer.close()
        })
  }

  private def printErrors(rdd: RDD[(Host, Span, Map[String, String])]) = {
    rdd.foreachPartition(
      partitionOfRecords =>
        {
          partitionOfRecords.foreach {
            x =>
              {
                val host = x._1
                val span = x._2
                val tagMap = x._3

                val endpoint = StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span)
                
                val traceId = span.getTraceId
                val httpStatusCode = tagMap.getOrElse("http.status_code", "not set").asInstanceOf[String];
                val errorMessage = tagMap.getOrElse("error", "not set")
                val errorJSON = "{\", \"endpoint\":\"" + endpoint + "\", \"traceId\":\"" + traceId + "\", \"status_code\":\"" + httpStatusCode + "\", \"errorMessage\":\"" + errorMessage + "\"}"
                println("Error: " + errorJSON)
              }
          }
        })
  }

}

