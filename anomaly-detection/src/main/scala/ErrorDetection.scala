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

  //Kafka
  val kafkaServers = "localhost:9092"
  val defaultErrorOutputTopic = "error"

  def errorDetection(spanStream: DStream[(Host, Span)], writeToKafka: Boolean) = {

    val spanTagStream = spanStream.map(x => (x._1, x._2, x._2.tags().asScala.map(y => y))) //map required to prevent some weird serialization issue

    //returns a map of tags of all spans that have the tag status_code set and which have not the value 200 or 201
    val spanErrorStream = spanTagStream.filter(x => errorFilter(x))

    spanErrorStream.foreachRDD(rdd => {
      if(writeToKafka){
        printErrorsAndWriteToTopic(rdd, defaultErrorOutputTopic)
      }else{
        printErrors(rdd)
      }
      
    })

    Logger.getRootLogger.setLevel(rootLoggerLevel)
  }
  
  private def errorFilter(input: (Host, Span, Map[String, String])) = {
    val status_code = input._3.get("http.status_code").getOrElse("").asInstanceOf[String]
    //filter out every Span that has a white listed status code and no error tag set
      if ((status_code.isEmpty() || whitelistedStatusCodes.contains(status_code))&&input._2.tags().get("error")==null) false
      else true
  }

  private def printErrorsAndWriteToTopic(rdd: RDD[(Host, Span, Map[String, String])], errorOutputTopic: String) = {
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

                  val hostString = host.getServiceName + "@" + host.getAddress + ":" + host.getPort
                  val spanName = x._1.getServiceName + "-" + x._2.tags().get("http.method") + ":" + x._1.getAddress + ":" + x._1.getPort + x._2.getName
                  val traceId = span.getTraceId
                  val httpStatusCode = tagMap.getOrElse("http.status_code", "").asInstanceOf[String];
                  val errorMessage = tagMap.getOrElse("error", "not set")
                  val errorJSON = "{\"host\":\"" + hostString + "\", \"spanName\":\"" + spanName + "\", \"traceId\":\"" + traceId + "\", \"status_code\":\"" + httpStatusCode + "\", \"errorMessage\":\"" + errorMessage + "\"}"

                  val message = new ProducerRecord[String, String](errorOutputTopic, null, errorJSON)
                  producer.send(message)
                  println("Error: "+errorJSON)
                }
            }
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

                  val hostString = host.getServiceName + "@" + host.getAddress + ":" + host.getPort
                  val spanName = x._1.getServiceName + "-" + x._2.tags().get("http.method") + ":" + x._1.getAddress + ":" + x._1.getPort + x._2.getName
                  val traceId = span.getTraceId
                  val httpStatusCode = tagMap.getOrElse("http.status_code", "").asInstanceOf[String];
                  val errorMessage = tagMap.getOrElse("error", "not set")
                  val errorJSON = "{\"host\":\"" + hostString + "\", \"spanName\":\"" + spanName + "\", \"traceId\":\"" + traceId + "\", \"status_code\":\"" + httpStatusCode + "\", \"errorMessage\":\"" + errorMessage + "\"}"
                  println("Error: "+errorJSON)
                }
            }
          })
  }

}

