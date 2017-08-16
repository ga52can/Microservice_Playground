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
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.stat.Statistics

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
  def train(ssc: StreamingContext, spanStream: DStream[(Host, Span)]): Set[(String, (KMeansModel, Double, Double, Double, Double, Array[Double]))] = {

    Logger.getRootLogger.setLevel(rootLoggerLevel)

    var filterRdd = ssc.sparkContext.emptyRDD[String]

    //    var vectorRdd = ssc.sparkContext.emptyRDD[(String, Vector)]

    var kMeansInformationRdd = ssc.sparkContext.emptyRDD[(String, Long, Long, Long, Double)] //identifier for splitting, begin(yyyy/MM/dd-HH:mm), duration

    val spanNameStream = StreamUtil.getSpanNameStreamFromSpanStream(spanStream)

    //fill the filterRdd with a distinct list of all spanNames that appear in spanNameStream
    spanNameStream.foreachRDD(rdd => {
      filterRdd = filterRdd.union(rdd).distinct()
    })

    val kMeansInformationStream = StreamUtil.getkMeansInformationStream(spanStream)

    println(StreamUtil.unixMilliToDateTimeStringMilliseconds(System.currentTimeMillis()) + " : Starting extracting kMeansInformationStream to RDD")
    kMeansInformationStream.foreachRDD { rdd =>
      //      println(rdd.count())
      if (rdd.count() == 0) {
        flag = 1
      }
      kMeansInformationRdd = kMeansInformationRdd.union(rdd)

    }

    //    val spanDurationVectorStream = StreamUtil.getSpanDurationVectorStreamFromSpanStream(spanStream)

    //    spanDurationVectorStream.foreachRDD { rdd =>
    //      if (rdd.count() == 0) {
    //        flag = 1
    //      }
    //      println(rdd.count())
    //      vectorRdd = vectorRdd.union(rdd)
    //
    //    }

    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()

    while (flag == 0) {
      //      println("flag: " + flag)
      Thread.sleep(1000)
    }

    val sc = ssc.sparkContext
    println(StreamUtil.unixMilliToDateTimeStringMilliseconds(System.currentTimeMillis()) + " : Finished extracting kMeansInformationStream to RDD")
    kMeansInformationRdd = kMeansInformationRdd.cache()

    println(StreamUtil.unixMilliToDateTimeStringMilliseconds(System.currentTimeMillis()) + " : Entering rpm calculation")
    kMeansInformationRdd.sortBy(_._2, true)

    var kMeansFeatureListBuffer: ListBuffer[(String, Long, Long, Long, Double)] = ListBuffer()
    var buffer: collection.mutable.Map[String, Array[Long]] = collection.mutable.Map()

    //this part is decreasing in performance the higher the amount of requests per the defined time interval is
    kMeansInformationRdd.collect.foreach(x => {
      val ts = x._2
      val identifier = x._1 //make sure the identifier represents actually the entity that receives the traffic -e.g. machine (IP?) not only endpoint

      //      val identifier = "system"
      var bufferInstance = buffer.get(identifier).getOrElse(Array[Long]())

      bufferInstance = bufferInstance ++ Array(ts)

      bufferInstance = bufferInstance.filter(p => p > ts - 60000)

      buffer.put(identifier, bufferInstance)

      val rpm = bufferInstance.size.toLong

      kMeansFeatureListBuffer.append((identifier, x._3, rpm, x._4, x._5)) //identifier, duration, rpm, memory, cpu

    })

    val kMeansFeatureRdd = sc.parallelize(kMeansFeatureListBuffer)
    println(StreamUtil.unixMilliToDateTimeStringMilliseconds(System.currentTimeMillis()) + " : Finished rpm calculation")

//    val stat = Statistics.colStats(kMeansFeatureRdd.map(x => Vectors.dense(x._2, x._3, x._4, x._5)))
//
//    println("Max: " + stat.max.toJson)
//    println("Min: " + stat.min.toJson)
//    println("Mean: " + stat.mean.toJson)
//    println("Variance: " + stat.variance.toJson)
//    println("NormL1: " + stat.normL1.toJson)
//    println("NormL1: " + stat.normL2.toJson)

    println("kMeansFeatureRdd Count: " + kMeansFeatureRdd.count())

    kMeansInformationRdd.unpersist(true)

    //    val kMeansFeatureVectorRdd = kMeansFeatureRdd.map(x => (x._1, Vectors.dense(x._2, x._3)))

    //    val vectorRdd = kMeansFeatureRdd.map(x => (x._1, Vectors.dense(x._2, x._3, x._4, x._5)))
    //    vectorRdd.cache()

    println(StreamUtil.unixMilliToDateTimeStringMilliseconds(System.currentTimeMillis()) + " : Starting to train SplittedKMeansModel")

    kMeansFeatureRdd.cache()
    val splittedAndScaledFeatureSet = splitAndScale(kMeansFeatureRdd, filterRdd)

    val modelSet = trainModelsForFilterList(splittedAndScaledFeatureSet, sc)

    println(StreamUtil.unixMilliToDateTimeStringMilliseconds(System.currentTimeMillis()) + " : Finished Training SplittedKMeansModel")

    //    vectorRdd.unpersist(true)
    kMeansFeatureRdd.unpersist(true)
    modelSet

  }

  //If you want to predict only on httpSpanStream, make sure to filter SpanStream before handing over to this method
  def anomalyDetection(ssc: StreamingContext, spanStream: DStream[(Host, Span)], models: Map[String, (KMeansModel, Double, Double, Double, Double, Array[Double])], anomalyOutputTopic: String, kafkaServers: String, printAnomaly: Boolean, writeToKafka: Boolean) = {

    var rpmMap: collection.mutable.Map[String, Long] = collection.mutable.Map()

    //    val fullInformationSpanDurationVectorStream = StreamUtil.getFullInformationSpanDurationVectorStreamFromSpanStream(spanStream)
    val spanNameStream = StreamUtil.getSpanNameStreamFromSpanStream(spanStream)

    val requestsPerMinute = spanNameStream.countByValueAndWindow(Seconds(60), Seconds(5)).foreachRDD(rdd => {

      val collectedRdd = rdd.collect()
      if (collectedRdd.size > 0) {
        for (rpmEntry <- collectedRdd) {
          rpmMap.put(rpmEntry._1, rpmEntry._2)
        }

      }

    })

    spanStream.foreachRDD { rdd =>
      rdd.foreachPartition(partition => {
        //creating a consumer is expensive - therefore create it only once per partition and not
        //once per RDD line 
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)

        //option to put the creation of the kafka message producer to be only created for each partition if performance would matter
        partition.foreach(x => {

          if (isAnomaly(x, models, rpmMap)) {
            reportAnomaly(x._1, x._2, anomalyOutputTopic, kafkaServers, producer, printAnomaly, writeToKafka)
          } //else {
          //          val identifier = StreamUtil.getEndpointIdentifierFromHostAndSpan(x._1, x._2)
          //          println("no anomaly: " + identifier + " rpm: " + rpmMap.get(identifier).getOrElse(0))
          //        }
        })

      })

    }
  }

  private def isAnomaly(datapoint: (Host, Span), models: Map[String, (KMeansModel, Double, Double, Double, Double, Array[Double])], rpmMap: collection.mutable.Map[String, Long]): Boolean = {

    val span = datapoint._2
    val host = datapoint._1
    val endpointIdentifier = StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span)
    val defaultLong: Long = 0
    val modelTuple = models.get(endpointIdentifier).getOrElse(null)

    if (modelTuple != null) {
      val model = modelTuple._1
      val threshold = modelTuple._2 //tuple: (model, ninetynine, median, avg, max) - of SQUAREDISTSANCE
      val centroid = model.clusterCenters(0) //only works as long as there is only one cluster

      val durationScalingDivisor = modelTuple._6(0) //get first Value in Scaling Divisor Array => for duration
//      val vector = Vectors.dense(span.getAccumulatedMicros.toDouble / durationScalingDivisor, rpmMap.getOrElse(endpointIdentifier, defaultLong).toDouble, span.tags().getOrDefault("jvm.memoryUtilization", "0").toDouble, span.tags().getOrDefault("cpu.system.utilizationAvgLastMinute", "0").toDouble)

//      val vector = Vectors.dense(span.getAccumulatedMicros.toDouble / durationScalingDivisor, rpmMap.getOrElse(endpointIdentifier, defaultLong).toDouble)
      val vector = Vectors.dense(span.getAccumulatedMicros.toDouble / durationScalingDivisor)
      
      
      var distance = Vectors.sqdist(centroid, vector)

      distance > threshold
    } else {
      //What to do when there is no model for a specific endpoint?
      //true: it is an anomaly
      //false: it is not an anomaly - e.g. just ignore it
      false
    }

  }

  private def reportAnomaly(host: Host, span: Span, anomalyOutputTopic: String, kafkaServers: String, producer: KafkaProducer[String, String], printAnomaly: Boolean, writeToKafka: Boolean) = {

    val anomalyDescriptor = "splittedKMeans"

    val anomalyJSON = StreamUtil.generateAnomalyJSON(host, span, anomalyDescriptor)

    if (printAnomaly) {
      println("anomaly: " + StreamUtil.getEndpointIdentifierFromHostAndSpan(host, span) + " - duration:"+span.getAccumulatedMicros)
    }

    if (writeToKafka) {

      val message = new ProducerRecord[String, String](anomalyOutputTopic, null, anomalyJSON)
      producer.send(message)
    }
  }

  private def trainModelsForFilterList(filteredFeatureSet: Set[(String, RDD[(String, Long, Long, Long, Double)], Array[Double])], sc: SparkContext) = {

    var resultSet = Set[(String, (KMeansModel, Double, Double, Double, Double, Array[Double]))]()
    for (entryOfFilteredFeatureSet <- filteredFeatureSet) {

      
//      val vectorRdd = entryOfFilteredFeatureSet._2.map(x => (Vectors.dense(x._2, x._3, x._4, x._5)))
//      val vectorRdd = entryOfFilteredFeatureSet._2.map(x => (Vectors.dense(x._2, x._3)))
      val vectorRdd = entryOfFilteredFeatureSet._2.map(x => (Vectors.dense(x._2)))
      val cachedVectorRdd = vectorRdd.cache()
      resultSet = resultSet.union(Set((entryOfFilteredFeatureSet._1, processEntryOfFilteredMap(cachedVectorRdd, sc, entryOfFilteredFeatureSet._3))))
      cachedVectorRdd.unpersist(true)
    }

    resultSet
  }

  private def splitAndScale(featureRdd: RDD[(String, Long, Long, Long, Double)], filterRdd: RDD[String]): Set[(String, RDD[(String, Long, Long, Long, Double)], Array[Double])] = {

    val filterArray = filterRdd.collect()
    val filterSet = filterArray.toSet

    val filteredMap = filterSet.map(key => key -> featureRdd.filter(x => x._1.equals(key)).map(y => y))

    var resultSet = Set[(String, RDD[(String, Long, Long, Long, Double)], Array[Double])]()
    for (entryOfFilteredMap <- filteredMap) {

      val scaledRddTuple = scale(entryOfFilteredMap._2)
      resultSet = resultSet.union(Set((entryOfFilteredMap._1, scaledRddTuple._1, scaledRddTuple._2)))
    }

    resultSet
  }

  private def scale(featureRdd: RDD[(String, Long, Long, Long, Double)]) = {

//    val durationRdd = featureRdd.map(x => x._2.toDouble)
//    val durationDivisor = rddMedian(durationRdd) / 1000
    
    val durationDivisor = 1

    val scalingFactors: Array[Double] = Array(durationDivisor, 1, 1, 1)

    val scaledRdd = featureRdd.map(x => (x._1, (x._2 / durationDivisor).ceil.toLong, x._3, x._4, x._5))

    (scaledRdd, scalingFactors)

  }

  private def processEntryOfFilteredMap(vectorRdd: RDD[Vector], sc: SparkContext, scalingParams: Array[Double]) = {
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
    val mas = modelAndStatistics(rdd999, model999)
    (mas._1, mas._2, mas._3, mas._4, mas._5, scalingParams)
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

