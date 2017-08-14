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


import scala.concurrent.Future
import java.sql.Connection
import java.sql.DriverManager

object RootCauseMain {

  //  val jsonObject = JSON.parseFull("...")
  //val globalMap = x.get.asInstanceOf[Map[String, Any]]
  //val reviewMap = globalMap.get("reviewDetails").get.asInstanceOf[Map[String, Any]]
  //val reviewScore = reviewMap.get("review_score").get.asInstanceOf[Double]

  //Logger
  val rootLoggerLevel = Level.WARN

  //Kafka
  val kafkaServers = "localhost:9092"
  val sleuthInputTopic = "sleuth"
  val anomalyOutputTopic = "anomalies"
  //  val kafkaAutoOffsetReset = "earliest" //use "earliest" to read as much from queue as possible or "latest" to only get new values

  //Spark
  val sparkAppName = "RootCause"
  val sparkMaster = "local[4]"
  val sparkLocalDir = "C:/tmp"
  val batchInterval = 5
  val checkpoint = "checkpoint"

  //global fields
  def main(args: Array[String]) {


    //  def call(wsClient: WSClient): Future[Unit] = {
    //    wsClient.url("http://www.google.com").get().map { response =>
    //      val statusText: String = response.statusText
    //      println(s"Got a response $statusText")
    //    }

    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
      .set("spark.local.dir", sparkLocalDir)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.kryoserializer.buffer.max", "1024m")

    sparkConf.registerKryoClasses(Array(classOf[Span]))
    sparkConf.registerKryoClasses(Array(classOf[Spans]))
    sparkConf.registerKryoClasses(Array(classOf[java.util.Map[String, String]]))

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))
    val sc = ssc.sparkContext
    ssc.checkpoint(checkpoint)
    Logger.getRootLogger.setLevel(rootLoggerLevel)

    val anomalyStream = StreamUtil.getAnomalyStreamFromKafka(ssc, "earliest", "rca4", kafkaServers, anomalyOutputTopic)
    anomalyStream.count().print()

    //find spans with the same SpanId and combine their errorMessage to handle them as one Span thereafter
    val combinedBySpanId = StreamUtil.combineAnomaliesBySpanId(anomalyStream)
    combinedBySpanId.count().print()

    //groupSpans by TraceID
    val aggregatedAnomalyStream = StreamUtil.getAnomaliesAggregatedByTraceId(combinedBySpanId)

    aggregatedAnomalyStream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {

        // connect to the database named "mysql" on the localhost
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost/anomalies"
        val username = "root"
        val password = "root"

        // there's probably a better way to do this
        var connection: Connection = null

        try {
          // make the connection
          Class.forName(driver)
          connection = DriverManager.getConnection(url, username, password)

        

        partition.foreach(x => {
          val leaves = x.getLeaves()
          for (anomaly <- leaves) {
            println("-------------------\nanomaly: " + anomaly.endpointIdentifier + "\ncalledBy: " + anomaly.printCalling() + "\n-------------------")
            if(connection!=null){
            val statement = connection.createStatement()
            val query= "INSERT INTO anomalies.anomalies (endpoint, information) VALUES ('" + anomaly.endpointIdentifier + "','" + anomaly.printCallingSQL() + "');"
            statement.executeUpdate(query)
            
            println("Exdecuted:" +query)
            }else{
              println("No SQL Connection available")
            }
          }

        })
        if(connection!=null){
          connection.close()
        }
          } catch {
          case e => e.printStackTrace
        }
        
      })

    })

    ssc.start()

    ssc.awaitTermination()

  }

}

