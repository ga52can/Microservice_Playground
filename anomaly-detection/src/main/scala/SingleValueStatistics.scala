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
import org.apache.spark.mllib.stat.Statistics

object SingleValueStatistics {

  //kMeans
  val k = 1
  var flag = 0

  //Logger
  val rootLoggerLevel = Level.WARN

  /**
   * If you want to train only on httpSpanStream,
   * make sure to filter SpanStream before handing over
   * to this method
   */
  def train(ssc: StreamingContext, spanStream: DStream[(Host, Span)]): Set[(String, (Long, Long, Long, Long, Long, Long, Long, Long))] = {

    Logger.getRootLogger.setLevel(rootLoggerLevel)

    var filterRdd = ssc.sparkContext.emptyRDD[String]

    var longRdd = ssc.sparkContext.emptyRDD[(String, Long)]

    val spanNameStream = StreamUtil.getSpanNameStreamFromSpanStream(spanStream)  
      
    //fill the filterRdd with a distinct list of all spanNames that appear in spanNameStream
    spanNameStream.foreachRDD(rdd => {
      filterRdd = filterRdd.union(rdd).distinct()
    })

    val spanDurationStream = StreamUtil.getSpanDurationStreamFromSpanStream(spanStream)

    spanDurationStream.foreachRDD { rdd =>
      if (rdd.count() == 0) {
        flag = 1
      }
      println(rdd.count())
      longRdd = longRdd.union(rdd)

    }

    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()

    while (flag == 0) {
      //      println("flag: " + flag)
      Thread.sleep(1000)
    }
    
    println("left loop")
    
    val sc = ssc.sparkContext
    longRdd.cache()

    val modelSet = getStatisticsForFilterList(longRdd, filterRdd, sc)

    longRdd.unpersist(true)

    modelSet

  }

  //If you want to predict only on httpSpanStream, make sure to filter SpanStream before handing over to this method
  def anomalyDetection(ssc: StreamingContext, spanStream: DStream[(Host, Span)], models: Map[String, (Long, Long, Long, Long, Long, Long, Long, Long)], anomalyOutputTopic: String, kafkaServers: String,  printAnomaly: Boolean, writeToKafka: Boolean) = {

    
    val fullInformationSpanDurationStream = StreamUtil.getFullInformationSpanDurationStreamFromSpanStream(spanStream)
    fullInformationSpanDurationStream.foreachRDD { rdd =>

      rdd.foreach(x => {
        val host = x._1
        val span = x._2
        val duration = x._3

        if (isAnomaly(host, span, duration, models)){
          reportAnomaly(host, span,anomalyOutputTopic, kafkaServers, printAnomaly, writeToKafka)
        } else {
          println("no anomaly: " + StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span) + " Duration: " + duration )
        }
      })

    }
  }

  private def printResultLine(resultLine: (String, (KMeansModel, Double, Double, Double, Double))) = {

    val result = resultLine._2
    val spanName = resultLine._1

    val model = result._1
    val percentile99 = Math.sqrt(result._2)
    val median = Math.sqrt(result._3)
    val avg = Math.sqrt(result._4)
    val max = Math.sqrt(result._5)

    println("----Results for " + spanName + "----")
    for (i <- 0 until model.clusterCenters.length) {
      println("Centroid: " + model.clusterCenters(i))
    }
    println("----Distances----")
    println("99 percentile: " + percentile99)
    println("Median: " + median)
    println("Average: " + avg)
    println("Max: " + max)
  }

  private def isAnomaly(host: Host, span: Span, duration: Long, models: Map[String, (Long, Long, Long, Long, Long, Long, Long, Long)]): Boolean = {

    val endpointIdentifier= StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span)
    val model = models.get(endpointIdentifier).getOrElse(null) //(min, max, avg, twentyfive, median, seventyfive, ninetyfive, ninetynine)

    if (model != null) {
      val ninetynine = model._8
      duration > ninetynine
    } else {
      true
    }
  }

  private def reportAnomaly(host: Host, span: Span, anomalyOutputTopic: String, kafkaServers: String,  printAnomaly: Boolean, writeToKafka: Boolean) = {

    val anomalyDescriptor = "splittedKMeans"
    
    val anomalyJSON = StreamUtil.generateAnomalyJSON(host, span, anomalyDescriptor)
      
    if(printAnomaly){
      println("anomaly: "+StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span))
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
    }
  }

  private def getStatisticsForFilterList(vectorRdd: RDD[(String, Long)], filterRdd: RDD[String], sc: SparkContext) = {

    val filterArray = filterRdd.collect()
    val filterSet = filterArray.toSet

    val filteredMap = filterSet.map(key => key -> vectorRdd.filter(x => x._1.equals(key)).map(y => y._2))

    var resultSet = Set[(String, (Long, Long, Long, Long, Long, Long, Long, Long))]()
    for (entryOfFilteredMap <- filteredMap) {

      val statistics = processEntryOfFilteredMap(entryOfFilteredMap._2, sc)
      resultSet = resultSet.union(Set((entryOfFilteredMap._1, statistics)))
    }

    resultSet
  }

  /**
   * Returns touple: (min, max, avg, twentyfive, median, seventyfive, ninetyfive, ninetynine)
   */
  private def processEntryOfFilteredMap(longRdd: RDD[Long], sc: SparkContext) = {

    val max = longRdd.max()
    val min = longRdd.min()
    val avg = longRdd.reduce(_ + _) / longRdd.count()
    val twentyfive = longRdd.top(Math.ceil(longRdd.count() * 0.75).toInt).last
    val median = longRdd.top(Math.ceil(longRdd.count() * 0.5).toInt).last
    val seventyfive = longRdd.top(Math.ceil(longRdd.count() * 0.25).toInt).last
    val ninetyfive = longRdd.top(Math.ceil(longRdd.count() * 0.05).toInt).last
    val ninetynine = longRdd.top(Math.ceil(longRdd.count() * 0.01).toInt).last

    val statisticalSummary = Statistics.colStats(longRdd.map(x => Vectors.dense(x)))
    val variance = statisticalSummary.variance.toArray(0)

    val statistics = (min, max, avg, twentyfive, median, seventyfive, ninetyfive, ninetynine)
    statistics
  }


  private def printStatistics(endpointName: String, min: Long, max: Long, avg: Long, twentyfive: Long, median: Long, seventyfive: Long, ninetyfive: Long, ninetynine: Long) = {

    println("----Statistics for: " + endpointName + "----")

    println("Min: " + min)
    println("Max: " + max)
    println("Avg: " + avg)
    println("25% percentile: " + twentyfive)
    println("Median: " + median)
    println("75% percentile: " + seventyfive)
    println("95% percentile: " + ninetyfive)
    println("99% percentile: " + ninetynine)
  }

}

