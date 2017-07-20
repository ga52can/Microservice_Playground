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

object KafkaKMeans {
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
  val errorOutputTopic = "error"
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
    trainingSsc.checkpoint(checkpoint)
    //val predictor = (model, ninetynine, median, avg, max)
    val result = trainKMeans(trainingSsc);

    val model = result._1
    val percentile99 = result._2
    val median = result._3
    val avg = result._4
    val max = result._5

    println("----Result----")
    for (i <- 0 until model.clusterCenters.length) {
      println("Centroid: " + model.clusterCenters(i))
    }
    println("----SquareDistances----")
    println("99 percentile: " + percentile99)
    println("Median: " + median)
    println("Average: " + avg)
    println("Max: " + max)

    val context = trainingSsc.sparkContext
    trainingSsc.stop(false)
    println("SSC stopped")
    //    val predConf = new SparkConf().setAppName("prediction").setMaster(sparkMaster)
    //      .set("spark.local.dir", sparkLocalDir)
    //      .set("spark.driver.allowMultipleContexts", "true")
    //
    //    predConf.registerKryoClasses(Array(classOf[Span]))
    //    predConf.registerKryoClasses(Array(classOf[Spans]))
    //    predConf.registerKryoClasses(Array(classOf[java.util.Map[String, String]]))

    val predictionSsc = new StreamingContext(context, Seconds(batchInterval))
    predictionSsc.checkpoint("pred-checkpoint")


    val threshold = percentile99

    kMeansAnomalyDetection(predictionSsc, model, threshold)

  }

  def kMeansAnomalyDetection(ssc: StreamingContext, model: KMeansModel, threshold: Double) = {
    val spanStream = getSpanStreamFromKafka(ssc, "latest", "prediction")

    val labeledSpanDurationVectorStream = getLabeledSpanDurationStreamFromSpanStream(spanStream)

    labeledSpanDurationVectorStream.foreachRDD { rdd =>

      rdd.foreach(f => {
        if (isAnomaly(f._2, model, threshold)) {
          reportAnomaly(f._1)
        } else {
          println("no anomaly")
        }
      })

    }
    ssc.start()
    ssc.awaitTermination()
  }

  def isAnomaly(dataPoint: Vector, model: KMeansModel, squaredthreshold: Double): Boolean = {
    val centroid = model.clusterCenters(0) //only works as long as there is only one cluster

    var distance = Vectors.sqdist(centroid, dataPoint)

    distance > squaredthreshold

  }

  def reportAnomaly(id: Long) {
    println("Span " + id + " is an anomaly")
  }

  def trainKMeans(ssc: StreamingContext): (KMeansModel, Double, Double, Double, Double) = {
    Logger.getRootLogger.setLevel(rootLoggerLevel)

    var buffer = new ArrayBuffer[Vector]
    buffer.append(Vectors.dense(1000.0))
    var vectorRdd = ssc.sparkContext.parallelize(buffer)

    val spanStream = getSpanStreamFromKafka(ssc, "earliest", "training")

    val spanDurationVectorStream = getSpanDurationStreamFromSpanStream(spanStream)

    spanDurationVectorStream.foreachRDD { rdd =>
      if (rdd.count() == 0) {
        flag = 1
        //        println("set flag to 1")
      }
      println(rdd.count())
      vectorRdd = vectorRdd.union(rdd)

    }

    //    spanDurationStream.saveAsTextFiles("data/test", ".txt")

    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()
    //    ssc.awaitTermination()

    while (flag == 0) {
      println("flag: " + flag)
      Thread.sleep(1000)
    }
    val sc = ssc.sparkContext
    println("left loop")
    //     println(vectorRdd.count())
    vectorRdd.cache()
    //     val normalizedData = normalizationOfDataBuffer(vectorRdd, sc)

    val dim = vectorRdd.first().toArray.size

    val zeroVector = Vectors.dense(Array.fill(dim)(0d))

    val distanceToZeroVector = vectorRdd.map(d => (distToCentroid(d, zeroVector), d))

    implicit val sortAscending = new Ordering[(Double, Vector)] {
      override def compare(a: (Double, Vector), b: (Double, Vector)) = {
        //a.toString.compare(b.toString)
        if (a._1 < b._1) {
          -1
        } else {
          +1
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
    printStatistics(rdd999, model999, " 99.9 percentile")
    //     printStatistics(rdd99, model99, " 99 percentile")
    //     printStatistics(rdd95, model95, " 95 percentile")

    vectorRdd.unpersist(true)

    //     val predictor = (model, ninetynine, median, avg, max)

    modelAndStatistics(rdd999, model999)

  }

  /**
   * autoOffsetReset can either be:
   * "earliest": Reads from the earliest possible value in kafka topic
   * "latest": Reads only the most recent values from kafka topic
   *
   */
  //TODO: Think about making autoOffsetReset an enum
  def getSpanStreamFromKafka(ssc: StreamingContext, autoOffsetReset: String, groupId: String): DStream[(Host, Span)] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(sleuthInputTopic)

    val sleuthWithHeader = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value())
    
    //    Would be a better way if it was not necessary to create a stream
    //    val sleuthBatch = KafkaUtils.createRDD(ssc.sparkContext, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent)

    val json = sleuthWithHeader.map(x => x.substring(x.indexOf("{\"host\":")))
    //    json.print(1000)

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
    //     spanStream.count().print(50);
    //     spanStream.count().print(); 

    spanStream

  }

  def getSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[Vector] = {
    val filteredSpanStream = filterSpanStream(spanStream)
    val spanDurationVectorStream = filteredSpanStream.map(x => Vectors.dense(x._2.getAccumulatedMicros))
    //    val spanDurationStream = filteredSpanStream.map(x => x._2.getAccumulatedMicros)
    spanDurationVectorStream
  }

  def getLabeledSpanDurationStreamFromSpanStream(spanStream: DStream[(Host, Span)]): DStream[(Long, Vector)] = {
    val filteredSpanStream = filterSpanStream(spanStream)
    val labledSpanDurationVectorStream = filteredSpanStream.map(x => (x._2.getSpanId, Vectors.dense(x._2.getAccumulatedMicros)))
    labledSpanDurationVectorStream
  }

  def filterSpanStream(spanStream: DStream[(Host, Span)]): DStream[(Host, Span)] = {
    spanStream.filter { x => x._2.getName.equals(spanNameFilter) }
  }

  def printStatistics(vectorRdd: RDD[Vector], model: KMeansModel, modelDescription: String) = {

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
  //returns the following tuple: (model, ninetynine, median, avg, max) - of SQUAREDISTSANCE
  def modelAndStatistics(vectorRdd: RDD[Vector], model: KMeansModel) = {

    //       val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
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
   * Normalization function.
   * Normalize the training data.
   */
  def normalizationOfDataBuffer(data: RDD[Vector], sc: SparkContext): RDD[Vector] = {
    //    val data = sc.parallelize(dataBuffer)
    val dataArray = data.map(_.toArray)
    val numCols = dataArray.first().length
    val n = dataArray.count()
    val sums = dataArray.reduce((a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataArray.fold(new Array[Double](numCols))(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2))
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }

    stdevs.foreach(f => println("stdevs: " + f))
    val means = sums.map(_ / n)
    means.foreach(f => println("means: " + f))
    def normalize(v: Vector): Vector = {
      val normed = (v.toArray, means, stdevs).zipped.map {
        case (value, mean, 0) => (value - mean) / 1 // if stdev is 0
        case (value, mean, stdev) => (value - mean) / stdev
      }
      Vectors.dense(normed)
    }

    val normalizedData = data.map(normalize(_)) // do nomalization
    normalizedData
  }

  /**
   * Train a KMean model using normalized data.
   */
  def trainModel(normalizedData: RDD[Vector]): KMeansModel = {
    val kmeans = new KMeans()
    kmeans.setK(k) // find that one center
    kmeans.setMaxIterations(100)
    val model = kmeans.run(normalizedData)
    model
  }

  def distToCentroid(datum: Vector, centroid: Vector): Double = {
    //  
    Vectors.sqdist(datum, centroid)
  }

  def rddMedian(rdd: RDD[Double]): Double = {
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

