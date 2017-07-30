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

object KafkaSplittedKMeans {

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
  def trainKMeansOnInitialSpanStream(ssc: StreamingContext, spanStream: DStream[(Host, Span)]): Set[(String, (KMeansModel, Double, Double, Double, Double))] = {

    Logger.getRootLogger.setLevel(rootLoggerLevel)

    var filterRdd = ssc.sparkContext.emptyRDD[String]

    var vectorRdd = ssc.sparkContext.emptyRDD[(String, Vector)]
    
    val spanNameStream = spanStream.map(x => x._1.getServiceName + "-" + x._2.tags().get("http.method") + ":" + x._1.getAddress + ":" + x._1.getPort + x._2.getName)

    //fill the filterRdd with a distinct list of all spanNames that appear in spanNameStream
    spanNameStream.foreachRDD(rdd => {
      filterRdd = filterRdd.union(rdd).distinct()
    })

    val spanDurationVectorStream = getSpanDurationStreamFromSpanStream(spanStream)

    spanDurationVectorStream.foreachRDD { rdd =>
      if (rdd.count() == 0) {
        flag = 1
      }
      println(rdd.count())
      vectorRdd = vectorRdd.union(rdd)

    }

    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()

    while (flag == 0) {
      //      println("flag: " + flag)
      Thread.sleep(1000)
    }
    val sc = ssc.sparkContext
    println("left loop")
    vectorRdd.cache()

    val modelSet = trainModelsForFilterList(vectorRdd, filterRdd, sc)

    vectorRdd.unpersist(true)

    modelSet

  }

  //If you want to predict only on httpSpanStream, make sure to filter SpanStream before handing over to this method
  def kMeansAnomalyDetection(ssc: StreamingContext, spanStream: DStream[(Host, Span)], models: Map[String, (KMeansModel, Double, Double, Double, Double)]) = {

    val labeledSpanDurationVectorStream = getLabeledSpanDurationStreamFromSpanStream(spanStream)

    labeledSpanDurationVectorStream.foreachRDD { rdd =>

      rdd.foreach(x => {
        if (isAnomaly(x, models)) {
          reportAnomaly(x._2, x._1)
        } else {
          println("no anomaly: " + x._1)
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

  private def isAnomaly(dataPoint: (String, Long, Vector), models: Map[String, (KMeansModel, Double, Double, Double, Double)]): Boolean = {
    val modelTuple = models.get(dataPoint._1).getOrElse(null)

    if (modelTuple != null) {
      val model = modelTuple._1
      val threshold = modelTuple._2 //tuple: (model, ninetynine, median, avg, max) - of SQUAREDISTSANCE
      val centroid = model.clusterCenters(0) //only works as long as there is only one cluster

      var distance = Vectors.sqdist(centroid, dataPoint._3)

      distance > threshold
    } else {
      true
    }

  }

  private def reportAnomaly(id: Long, spanName: String) {
    println("anomaly: " + spanName + ": Span " + id + " is an anomaly")
  }

  private def trainModelsForFilterList(vectorRdd: RDD[(String, Vector)], filterRdd: RDD[String], sc: SparkContext) = {

    val filterArray = filterRdd.collect()
    val filterSet = filterArray.toSet

    val filteredMap = filterSet.map(key => key -> vectorRdd.filter(x => x._1.equals(key)).map(y => y._2))

    var resultSet = Set[(String, (KMeansModel, Double, Double, Double, Double))]()
    for (entryOfFilteredMap <- filteredMap) {

      resultSet = resultSet.union(Set((entryOfFilteredMap._1, processEntryOfFilteredMap(entryOfFilteredMap._2, sc))))
    }

    resultSet
  }

  private def processEntryOfFilteredMap(vectorRdd: RDD[Vector], sc: SparkContext) = {
    val dim = vectorRdd.first().toArray.size
    val zeroVector = Vectors.dense(Array.fill(dim)(0d))
    val distanceToZeroVector = vectorRdd.map(d => (distToCentroid(d, zeroVector), d))

    implicit val sortAscending = new Ordering[(Double, Vector)] {
      override def compare(a: (Double, Vector), b: (Double, Vector)) = {
        //a.toString.compare(b.toString)
        if (a._1 < b._1) {
          -1
        } else if (a._1 < b._1) {
          +1
        } else {
          0
        }
      }
    }

    //     val array95 = distanceToZeroVector.takeOrdered(Math.ceil(distanceToZeroVector.count()*0.95).toInt)(sortAscending).map(f=> f._2)
    //     val array99 = distanceToZeroVector.takeOrdered(Math.ceil(distanceToZeroVector.count()*0.99).toInt)(sortAscending).map(f=> f._2)
    val array999 = distanceToZeroVector.takeOrdered(Math.ceil(distanceToZeroVector.count() * 0.999).toInt)(sortAscending).map(f => f._2)

    //     val rdd95 = sc.parallelize(array95)
    //     val rdd99 = sc.parallelize(array99)
    val rdd999 = sc.parallelize(array999)

    //     val model = trainModel(vectorRdd)
    //     val model95 = trainModel(rdd95)
    //     val model99 = trainModel(rdd99)
    val model999 = trainModel(rdd999)

    //     val distances = normalizedData.map(d => distToCentroid(d, model))

    //     printStatistics(vectorRdd, model, "All data points")
    //    printStatistics(rdd999, model999, " 99.9 percentile")
    //     printStatistics(rdd99, model99, " 99 percentile")
    //     printStatistics(rdd95, model95, " 95 percentile")

    //     val predictor = (model, ninetynine, median, avg, max)
    modelAndStatistics(rdd999, model999)
  }

  private def getSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(String, Vector)] = {
    //    val filteredSpanStream = filterSpanStream(spanStream)
    val spanDurationVectorStream = spanStream.map(x => (x._1.getServiceName + "-" + x._2.tags().get("http.method") + ":" + x._1.getAddress + ":" + x._1.getPort + x._2.getName, Vectors.dense(x._2.getAccumulatedMicros)))

    //    val spanDurationVectorStream = spanStream.map(x => (x._2.getName,Vectors.dense(x._2.getAccumulatedMicros)))
    //    val spanDurationStream = filteredSpanStream.map(x => x._2.getAccumulatedMicros)
    spanDurationVectorStream
  }

  private def getLabeledSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(String, Long, Vector)] = {

    val labledSpanDurationVectorStream = spanStream.map(x => (x._1.getServiceName + "-" + x._2.tags().get("http.method") + ":" + x._1.getAddress + ":" + x._1.getPort + x._2.getName, x._2.getSpanId, Vectors.dense(x._2.getAccumulatedMicros)))
    labledSpanDurationVectorStream
  }

  private def printStatistics(vectorRdd: RDD[Vector], model: KMeansModel, modelDescription: String) = {

    //       val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    val centroid = model.clusterCenters(0) //if only 1 center

    println("----Model: " + modelDescription + "----")

    println("Centroid: " + centroid)

    val distances = vectorRdd.map(d => distToCentroid(d, centroid))

    val max = Math.sqrt(distances.max())
    val median = Math.sqrt(rddMedian(distances))
    val avg = Math.sqrt(distances.reduce(_ + _) / distances.count())
    val ninetyfive = Math.sqrt(distances.top(Math.ceil(distances.count() * 0.05).toInt).last)
    val ninetynine = Math.sqrt(distances.top(Math.ceil(distances.count() * 0.01).toInt).last)

    println("Max Distance: " + max)
    println("Median Distance: " + median)
    println("Avg Distance: " + avg)
    println("95% percentile: " + ninetyfive)
    println("99% percentile: " + ninetynine)
  }

  /**
   * returns the following tuple: (model, ninetynine, median, avg, max)
   *
   *  model: KMeansModel
   *  ninetyNine: 99 percentile of squaredistances between vectors and centroid
   *  median: median of squaredistances between vectors and centroid
   *  avg: average of squaredistances between vectors and centroid
   *  max: max of squaredistances between vectors and centroid
   *
   */
  private def modelAndStatistics(vectorRdd: RDD[Vector], model: KMeansModel) = {

    val centroid = model.clusterCenters(0) //if only 1 center

    val distances = vectorRdd.map(d => distToCentroid(d, centroid))

    val max = distances.max()
    val median = rddMedian(distances)
    val avg = distances.reduce(_ + _) / distances.count()
    val ninetyfive = distances.top(Math.ceil(distances.count() * 0.05).toInt).last
    val ninetynine = distances.top(Math.ceil(distances.count() * 0.01).toInt).last

    val modelAndStatistics = (model, ninetynine, median, avg, max)
    modelAndStatistics
  }

  /**
   * Train a KMean model using normalized data.
   */
  private def trainModel(normalizedData: RDD[Vector]): KMeansModel = {
    val kmeans = new KMeans()
    kmeans.setK(k) // find that one center
    kmeans.setMaxIterations(100)
    val model = kmeans.run(normalizedData)
    model
  }

  private def distToCentroid(datum: Vector, centroid: Vector): Double = {
    Vectors.sqdist(datum, centroid)
  }

  private def rddMedian(rdd: RDD[Double]): Double = {
    var median: Double = 0.0
    val sorted = rdd.sortBy(identity).zipWithIndex().map {
      case (v, idx) => (idx, v)
    }

    val count = sorted.count()

    if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      median = (sorted.lookup(l).head + sorted.lookup(r).head) / 2
    } else {
      median = sorted.lookup(count / 2).head
    }
    median
  }

}

