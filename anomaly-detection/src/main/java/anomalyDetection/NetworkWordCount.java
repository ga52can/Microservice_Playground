//package anomalyDetection;
//
//import java.util.Arrays;
//import java.util.Iterator;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.*;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.*;
//import org.apache.spark.streaming.api.java.*;
//import scala.Tuple2;
//
//public class NetworkWordCount {
//
//	public static void main(String[] args) throws InterruptedException {
//
//		SparkConf conf = new SparkConf().setAppName("Network Word Count").setMaster("local[4]").set("spark.local.dir",
//				"C:/tmp");
//		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
//
//		JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 9999);
//		
//		Logger rootLogger = Logger.getRootLogger();
//	    rootLogger.setLevel(Level.ERROR);
//
//		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//			@Override
//			public Iterator<String> call(String x) {
//				return Arrays.asList(x.split(" ")).iterator();
//			}
//		});
//
//		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
//			@Override
//			public Tuple2<String, Integer> call(String s) {
//				return new Tuple2<>(s, 1);
//			}
//
//		});
//
//		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//			@Override
//			public Integer call(Integer i1, Integer i2) {
//				return i1 + i2;
//			}
//		});
//		
//		wordCounts.print();
//		
//		sc.start();
//		sc.awaitTermination();
//
//	}
//
//}
