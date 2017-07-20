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
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec

object KafkaSplittedKMeans {
  //General
  val whitelistedStatusCodes = Array("200", "201")
  val spanNameFilter = "http:/business-core-service/businesses/list"

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
  val kafkaAutoOffsetReset = "earliest" //use "earliest" to read as much from queue as possible or "latest" to only get new values

  //Spark
  val sparkAppName = "KafkaKMeans"
  val sparkMaster = "local[4]"
  val sparkLocalDir = "C:/tmp"
  val batchInterval = 5
  val checkpoint = "checkpoint"

  //global fields

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(rootLoggerLevel)

    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster).set("spark.local.dir", sparkLocalDir).set("spark.driver.allowMultipleContexts", "true")

    sparkConf.registerKryoClasses(Array(classOf[Span]))
    sparkConf.registerKryoClasses(Array(classOf[Spans]))
    sparkConf.registerKryoClasses(Array(classOf[java.util.Map[String, String]]))

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))
    ssc.checkpoint(checkpoint)
    var buffer = new ArrayBuffer[Vector]
    buffer.append(Vectors.dense(1000.0))
    var vectorRdd = ssc.sparkContext.parallelize(buffer)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "zipkinreader",
      "auto.offset.reset" -> kafkaAutoOffsetReset,
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

    
//    val spec = StateSpec.function(mappingFunction)
//    spec.
//    
//    spanStream.mapWithState(spec)

    val filteredSpanStream = spanStream.filter { x => x._2.getName.equals(spanNameFilter) }

    val spanDurationVectorStream = filteredSpanStream.map(x => Vectors.dense(x._2.getAccumulatedMicros))
    val spanLabledDurationVectorStream = spanStream.map(x => (x._2.getSpanId, Vectors.dense(x._2.getAccumulatedMicros)))
    val spanDurationStream = filteredSpanStream.map(x => x._2.getAccumulatedMicros)

    spanDurationVectorStream.foreachRDD { rdd =>
      if (rdd.count() == 0) {
        flag = 1
      }
      println(rdd.count())
      vectorRdd = vectorRdd.union(rdd)

    }

    //    spanDurationStream.saveAsTextFiles("data/test", ".txt")

    Logger.getRootLogger.setLevel(rootLoggerLevel)
    ssc.start()
    //    ssc.awaitTermination()

    while (flag == 0) {
    }
    val sc = ssc.sparkContext

    println(vectorRdd.count())

    val normalizedData = normalizationOfDataBuffer(vectorRdd, sc)
    val model = trainModel(normalizedData)

    val centroid = model.clusterCenters(0).toString // save centroid to file
    println(centroid)

    val distances = normalizedData.map(d => distToCentroid(d, model))

    val max = distances.max()
    val median = rddMedian(distances)
    val avg = distances.reduce(_ + _) / distances.count()

    println("Max Distance: " + max)
    println("Median Distance: " + median)
    println("Avg Distance: " + avg)
    //     
    //     ssc.stop(true, true)

    ssc.awaitTermination()

  }
  
//  // A mapping function that maintains an integer state and return a String
//    def mappingFunction(key: String, value: Option[(Host, Span)], state: State[Set[String]]): Option[(Host, Span)] = {
//      
//      var set: Set[String] = state.getOption().getOrElse(Set())
//      var updateSet = set + value.get
//      
//      
//      
//      // Use state.exists(), state.get(), state.update() and state.remove()
//      // to manage state, and return the necessary string
//    }


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

    print(stdevs)
    val means = sums.map(_ / n)
    print(means)
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

  def distToCentroid(datum: Vector, model: KMeansModel): Double = {
    //    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    val centroid = model.clusterCenters(0) //if only 1 center
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

