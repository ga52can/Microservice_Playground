package anomalyDetection;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaWordCount {

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("Network Word Count").setMaster("local[4]").set("spark.local.dir",
				"C:/tmp");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));

		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class); //For Objects most likely ByteArrayDeserializer should be used
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "test");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("spark-test");

		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		
		
		JavaPairDStream<String, String> messages = stream.mapToPair(
				  new PairFunction<ConsumerRecord<String, String>, String, String>() {
				    @Override
				    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				      return new Tuple2<>(record.key(), record.value());
				    }
				  });
		
//		messages.print();
		
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> x) throws Exception {
				
				return x._2;
			}
			
		});
		
//		lines.print();

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}

		});

		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.print();

		sc.start();
		sc.awaitTermination();

	}

}
