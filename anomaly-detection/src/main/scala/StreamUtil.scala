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
  
    def reportAnomaly(host: Host, span: Span, anomalyOutputTopic: String, kafkaServers: String, producer: KafkaProducer[String, String], printAnomaly: Boolean, writeToKafka: Boolean) = {

    val anomalyDescriptor = "splittedKMeans"

    val anomalyJSON = StreamUtil.generateAnomalyJSON(host, span, anomalyDescriptor)

    if (printAnomaly) {
      println(anomalyDescriptor+" - anomaly: " + StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span) + " - duration:" + span.getAccumulatedMicros)
    }

    if (writeToKafka) {

      val message = new ProducerRecord[String, String](anomalyOutputTopic, null, anomalyJSON)
      producer.send(message)
    }
  }

  def createKafkaProducer(kafkaServers: String): KafkaProducer[String, String] = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    producer
  }

  def getEndpointIdentifierFromHostAndSpan(host: Host, span: Span): String = {
    //    This identifier includes IP
    //    var identifier = host.getServiceName + "@" + host.getAddress  +  ":" + host.getPort + span.getName.substring(5) + ":::" +span.tags().getOrDefault("http.method", span.tags().get("mvc.controller.class")+span.tags().getOrDefault("mvc.controller.method", ""))
    //    Use this identifier when local generated test set was generated in different network or played with in different networs- ignores ip
    var identifier = host.getServiceName + "@ignoredIP:" + host.getPort + span.getName.substring(5) + ":::" + span.tags().getOrDefault("http.method", span.tags().get("mvc.controller.class") + span.tags().getOrDefault("mvc.controller.method", ""))

    identifier
  }

  def generateAnomalyJSON(host: Host, span: Span, anomalyDescriptor: String): String = {
    val endpointIdentifier = getEndpointIdentifierFromHostAndSpan(host, span)
    var spanId = span.getSpanId.toString
    //in an rpc case there are two spans with the same ID - one with http annotations and the other with mvc annotations
    //this if clause makes sure the IDs of those spans are unique once we get to root cause analysis
    //as all SpanIds seem to have 19 digits (plus an additional negative sign in some cases) * 1000 seems a good way to make them
    //unique and keep them as long
    if (span.tags().get("mvc.controller.class") != null) {
      spanId = "mvc" + spanId
    }
    val traceId = span.getTraceId.toString
    val begin = span.getBegin
    val end = span.getEnd
    var parentId = 0L
    if (span.getParents.size() > 0) {
      parentId = span.getParents.get(0)
    }
    val errorJSON = "{\"endpointIdentifier\":\"" + endpointIdentifier + "\", \"spanId\":\"" + spanId + "\", \"traceId\":\"" + traceId + "\", \"anomalyDescriptor\":\"" + anomalyDescriptor + "\", \"begin\":\"" + begin + "\", \"end\":\"" + end + "\", \"parentId\":\"" + parentId + "\"}"
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

    //Stream of all Spans that come from Kafka
    val spanStream = spansStream.flatMap(x => {
      var buffer = ArrayBuffer[(Host, Span)]()
      x.getSpans.asScala.foreach(y => buffer.append((x.getHost, y)))
      buffer
    })

    spanStream

  }

  def getAnomalyStreamFromKafka(ssc: StreamingContext, autoOffsetReset: String, groupId: String, kafkaServers: String, anomalyTopic: String): DStream[(String, String, Long, Long, String, String, String)] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(anomalyTopic)

    val anomalyJson = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value())
    //    {"endpointIdentifier":"endpointIdentifier", "spanId":"spanId", "traceId":"traceId", "anomalyDescriptor":"anomalyDescriptor","begin":"begin", "end":"end"}

    val anomalyStream = anomalyJson.map(x => {
      val jsonObject = JSON.parseFull(x)
      val anomalyMap = jsonObject.get.asInstanceOf[collection.immutable.Map[String, Any]]
      val endpointIdentifier = anomalyMap.get("endpointIdentifier").get.asInstanceOf[String]
      val spanId = anomalyMap.get("spanId").get.asInstanceOf[String]
      val traceId = anomalyMap.get("traceId").get.asInstanceOf[String]
      val anomalyDescriptor = anomalyMap.get("anomalyDescriptor").get.asInstanceOf[String]
      val begin = anomalyMap.get("begin").get.asInstanceOf[String].toLong
      val end = anomalyMap.get("end").get.asInstanceOf[String].toLong
      val parentId = anomalyMap.get("parentId").get.asInstanceOf[String]

      (spanId, traceId, begin, end, endpointIdentifier, anomalyDescriptor, parentId)

    })

    anomalyStream

  }

  def combineAnomaliesBySpanId(anomalyStream: DStream[(String, String, Long, Long, String, String, String)]): DStream[(String, String, Long, Long, String, String, String)] = {

    val aggregatedStream = anomalyStream.map(x => (x._1, (x._1, x._2, x._3, x._4, x._5, x._6, x._7))).reduceByKey((x, y) => (x._1, x._2, x._3, x._4, x._5, x._6 + ", " + y._6, x._7))
    aggregatedStream.map(x => x._2)
  }

  def getAnomaliesAggregatedByTraceId(anomalyTupleStream: DStream[(String, String, Long, Long, String, String, String)]): DStream[Anomaly] = {

    val groupedStream = anomalyTupleStream.map(x => (x._2, (x._1, x._2, x._3, x._4, x._5, x._6, x._7))).groupByKey()
    val anomalyStream = groupedStream.map(x => {
      //(spanId, traceId, begin, end, endpointIdentifier, anomalyDescriptor)
      var anomalyList = x._2.toList.sortBy(x => (x._3, x._4, x._1)) //add span id(x._1) as http/mvc spans can potentially have same start and end - as the mvc spans id gets an additional mvc as first letters when reporting to the anomaly queue this can be used to create the order in those cases
      var anomaly: Anomaly = null
      for (anomalyTuple <- anomalyList) {
        if (anomaly == null) {
          anomaly = new Anomaly(anomalyTuple._1, anomalyTuple._2, anomalyTuple._3, anomalyTuple._4, anomalyTuple._5, anomalyTuple._6, anomalyTuple._7)
        } else {
          anomaly.insert(new Anomaly(anomalyTuple._1, anomalyTuple._2, anomalyTuple._3, anomalyTuple._4, anomalyTuple._5, anomalyTuple._6, anomalyTuple._7))
        }
      }
      anomaly
    })
    anomalyStream
  }

  /**
   * EndpointIdentifier, Begin, AccumulatedMicros, jvm.memoryUtilization, cpu.system.utilizationAvgLastMinute
   */
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

