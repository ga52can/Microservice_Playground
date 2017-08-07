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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.joda.time.{ DateTimeZone }
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

object StreamUtil {

  def getEndpointIdentifierFromHostAndSpan(host: Host, span: Span): String = {
    var identifier = host.getServiceName + "-" + span.tags().get("http.method") + ":" + host.getAddress + ":" + host.getPort + span.getName
    identifier
  }

  def generateAnomalyJSON(host: Host, span: Span, anomalyDescriptor: String): String = {
    val endpointIdentifier = getEndpointIdentifierFromHostAndSpan(host, span)
    val spanId = span.getSpanId
    val traceId = span.getTraceId
    val begin = span.getBegin
    val end = span.getEnd

    val errorJSON = "{\"endpointIdentifier\":\"" + endpointIdentifier + "\", \"spanId\":\"" + spanId + "\", \"traceId\":\"" + traceId + "\", \"begin\":\"" + begin + "\", \"end\":\"" + end + "\"}"
    errorJSON

  }

  def unixMilliToDateTimeStringMilliseconds(unixMillis: Long): String = {
    val dateString = new DateTime(unixMillis).toDateTime.toString("yyyy/MM/dd-HH:mm:ss.SSS")
    dateString
  }
  
    def unixMilliToDateTimeStringMinutes(unixMillis: Long): String = {
    val dateString = new DateTime(unixMillis).toDateTime.toString("yyyy/MM/dd-HH:mm")
    dateString
  }

  /**
   * autoOffsetReset can either be:
   * "earliest": Reads from the earliest possible value in kafka topic
   * "latest": Reads only the most recent values from kafka topic
   *
   */
  //TODO: Think about making autoOffsetReset an enum
  def getSpanStreamFromKafka(ssc: StreamingContext, autoOffsetReset: String, groupId: String, kafkaServers: String, sleuthInputTopic: String): DStream[(Host, Span)] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(sleuthInputTopic)

    val sleuthWithHeader = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value())

    val json = sleuthWithHeader.map(x => x.substring(x.indexOf("{\"host\":")))

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

    spanStream

  }

  def getkMeansInformationStream(spanStream: DStream[(Host, Span)]): DStream[(String, Long, Long, Long, Double)] = {
    val kMeansInformationStream = spanStream.map(x => (getEndpointIdentifierFromHostAndSpan(x._1, x._2), x._2.getBegin, x._2.getAccumulatedMicros, x._2.tags().getOrDefault("jvm.memoryUtilization", "0").toLong, x._2.tags().getOrDefault("cpu.system.utilizationAvgLastMinute", "0").toDouble))
    kMeansInformationStream
  }

  def getSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(String, Long)] = {
    val spanDurationStream = spanStream.map(x => (getEndpointIdentifierFromHostAndSpan(x._1, x._2), x._2.getAccumulatedMicros))
    spanDurationStream
  }

  def getLabeledSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(String, Long, Long)] = {
    val labledSpanDurationVectorStream = spanStream.map(x => (getEndpointIdentifierFromHostAndSpan(x._1, x._2), x._2.getSpanId, x._2.getAccumulatedMicros))
    labledSpanDurationVectorStream
  }

  def getFullInformationSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(Host, Span, Long)] = {
    val fullInformationSpanDurationVectorStream = spanStream.map(x => (x._1, x._2, x._2.getAccumulatedMicros))
    fullInformationSpanDurationVectorStream
  }

  def getSpanDurationVectorStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(String, Vector)] = {
    val spanDurationVectorStream = spanStream.map(x => (getEndpointIdentifierFromHostAndSpan(x._1, x._2), Vectors.dense(x._2.getAccumulatedMicros)))
    spanDurationVectorStream
  }

  def getLabeledSpanDurationVectorStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(String, Long, Vector)] = {
    val labledSpanDurationVectorStream = spanStream.map(x => (getEndpointIdentifierFromHostAndSpan(x._1, x._2), x._2.getSpanId, Vectors.dense(x._2.getAccumulatedMicros)))
    labledSpanDurationVectorStream
  }

  def getFullInformationSpanDurationVectorStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(Host, Span, Vector)] = {
    val fullInformationSpanDurationVectorStream = spanStream.map(x => (x._1, x._2, Vectors.dense(x._2.getAccumulatedMicros)))
    fullInformationSpanDurationVectorStream
  }

  def getSpanNameStreamFromSpanStream(spanStream: DStream[(Host, Span)]) = {
    val spanNameStream = spanStream.map(x => getEndpointIdentifierFromHostAndSpan(x._1, x._2))
    spanNameStream
  }

  def filterSpanStreamForHttpRequests(spanStream: DStream[(Host, Span)]): DStream[(Host, Span)] = {
    spanStream.filter(x => x._2.tags().keySet().contains("http.method") || x._2.getSpanId == x._2.getTraceId)
  }

}

