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
  val anomalyOutputTopic = "anomalies"
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

    val trainingSpanStream = StreamUtil.getSpanStreamFromKafka(trainingSsc, "earliest", "training", kafkaServers, sleuthInputTopic)

    //    val trainingHttpSpanStream = filterSpanStreamForHttpRequests(trainingSpanStream)

    val splittedKMeansResult = KafkaSplittedKMeans.train(trainingSsc, trainingSpanStream)

    println("SplittedKmeans finished training")
    trainingSsc.stop(false, false)

    //    val statisticsPredictionSsc = new StreamingContext(sparkContext, Seconds(batchInterval))
    //    val statisticsTrainingSpanStream = StreamUtil.getSpanStreamFromKafka(statisticsPredictionSsc, "earliest", "statisticstraining", kafkaServers, sleuthInputTopic)
    //    val statisticsTrainingHttpSpanStream = StreamUtil.filterSpanStreamForHttpRequests(statisticsTrainingSpanStream)
    //    val singleValueStatisticsResult = SingleValueStatistics.train(statisticsPredictionSsc, statisticsTrainingHttpSpanStream)

    //    statisticsPredictionSsc.stop(false, false)
    //    println("SingleValueStatistics finished training")

    println("-----------------------------------------------------")
    println("-------------------SplittedKMeans--------------------")
    println("-----------------------------------------------------")
    for (resultLine <- splittedKMeansResult) {

      printResultLine(resultLine)
    }

    //    println("-----------------------------------------------------")
    //    println("----------------SingleValueStatistics----------------")
    //    println("-----------------------------------------------------")
    //    for (resultLine <- singleValueStatisticsResult) {
    //      val statistics = resultLine._2
    //
    //      printSingleValueStatistics(resultLine._1, statistics._1, statistics._2, statistics._3, statistics._4, statistics._5, statistics._6, statistics._7, statistics._8)
    //
    //    }

    val predictionSsc = new StreamingContext(sparkContext, Seconds(batchInterval))
    predictionSsc.checkpoint("pred-checkpoint")

    val splittedKMeansModels = splittedKMeansResult.toMap

    //    val singleValueStatisticsModels = singleValueStatisticsResult.toMap

    val predictionSpanStream = StreamUtil.getSpanStreamFromKafka(predictionSsc, "latest", "prediction", kafkaServers, sleuthInputTopic)

    //    val predictionHttpSpanStream = StreamUtil.filterSpanStreamForHttpRequests(predictionSpanStream)

    //        ErrorDetection.errorDetection(predictionSpanStream, anomalyOutputTopic, kafkaServers, true, false) //do not write to Kafka - just print errors

    //        FixedThreshold.monitorTagForFixedThreshold(predictionSpanStream, "cpu.system.utilization", 99, true, anomalyOutputTopic, kafkaServers, true, false)

    //        FixedThreshold.monitorTagForFixedThreshold(predictionSpanStream, "jvm.memoryUtilization", 15, true, anomalyOutputTopic, kafkaServers, true,  false)

    KafkaSplittedKMeans.anomalyDetection(predictionSsc, predictionSpanStream, splittedKMeansModels, anomalyOutputTopic, kafkaServers, true, false)

//    SingleValueStatistics.anomalyDetection(predictionSsc, predictionHttpSpanStream, singleValueStatisticsModels, anomalyOutputTopic, kafkaServers, true, false)

    predictionSsc.start()
    predictionSsc.awaitTermination()
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

  private def printSingleValueStatistics(endpointName: String, min: Long, max: Long, avg: Long, twentyfive: Long, median: Long, seventyfive: Long, ninetyfive: Long, ninetynine: Long) = {

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

