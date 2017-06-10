//package anomalyDetection;
//
//import java.io.UnsupportedEncodingException;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.ByteArrayDeserializer;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.*;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.*;
//import org.apache.spark.streaming.api.java.*;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import org.codehaus.jackson.map.DeserializationConfig.Feature;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.springframework.cloud.sleuth.Span;
//import org.springframework.cloud.sleuth.stream.Spans;
//
//import scala.Tuple2;
//
//public class KafkaReadZipkin {
//
//	public static void main(String[] args) throws InterruptedException {
//
//		SparkConf conf = new SparkConf().setAppName("Network Word Count").setMaster("local[4]").set("spark.local.dir",
//				"C:/tmp").set("spark.driver.extraJavaOptions", "agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005");
//		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));
//
//		Logger rootLogger = Logger.getRootLogger();
//		rootLogger.setLevel(Level.ERROR);
//
//		Map<String, Object> kafkaParams = new HashMap<>();
//		kafkaParams.put("bootstrap.servers", "localhost:9092");
//		kafkaParams.put("key.deserializer", ByteArrayDeserializer.class); //For Objects most likely ByteArrayDeserializer should be used
//		kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
//		kafkaParams.put("group.id", "test");
//		kafkaParams.put("auto.offset.reset", "earliest");
//		kafkaParams.put("enable.auto.commit", false);
//
//		Collection<String> topics = Arrays.asList("sleuth");
//
//		final JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = KafkaUtils.createDirectStream(sc,
//				LocationStrategies.PreferConsistent(),
//				ConsumerStrategies.<byte[], byte[]>Subscribe(topics, kafkaParams));
//		
//		
//		
//		JavaPairDStream<String, String> messages = stream.mapToPair(
//				  new PairFunction<ConsumerRecord<byte[], byte[]>, String, String>() {
//				    @Override
//				    public Tuple2<String, String> call(ConsumerRecord<byte[], byte[]> record) throws UnsupportedEncodingException {
////				    	System.out.println(record.value());
//				    	String key = null;
//				    	String value = null;
//				    	if(record.key()!=null){
//				    		key = new String(record.key(), "UTF-8");
//				    	}
//				    	if(record.value()!=null){
//				    		value = new String(record.value(), "UTF-8"); //convert ByteArray to String
//				    		int jsonStartIndex = value.indexOf("{\"host\":"); //The Json always starts with {"host":{
//				    		value = value.substring(jsonStartIndex); //remove the header before the real json begins
//				    	}
//				      return new Tuple2<>(key, value);
//				    }
//				  });
//		
////		messages.print();
//		JavaDStream<Spans> spansCollections = messages.map(new Function<Tuple2<String, String>, Spans>() {
//
//			@Override
//			public Spans call(Tuple2<String, String> x) throws Exception {
//				String json = x._2;
//				System.out.println(json);
//				ObjectMapper mapper = new ObjectMapper();
//				mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//				Spans spans = mapper.readValue(json, Spans.class);
//				
//				return spans;
//			}
//			
//		});
//		
//		spansCollections.print();
//		
//		JavaDStream<Span> spans = spansCollections.flatMap(
//				new FlatMapFunction<Spans, Span>() {
//
//					@Override
//					public Iterator<Span> call(Spans spans) throws Exception {
//						return spans.getSpans().iterator();
//					}
//					
//					
//				}
//				
//				);
//		
//		JavaPairDStream<Long, Long> spanPairs = spans.mapToPair(
//				new PairFunction<Span, Long, Long>() {
//
//					@Override
//					public Tuple2<Long, Long> call(Span span) throws Exception {
//						return new Tuple2<Long, Long>(span.getTraceId(), span.getSpanId());
//					}
//					
//				}
//				
//				);
//				
//		spanPairs.print();		
//		
//
////		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
////			@Override
////			public Iterator<String> call(String x) {
////				return Arrays.asList(x.split(" ")).iterator();
////			}
////		});
////
////		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
////			@Override
////			public Tuple2<String, Integer> call(String s) {
////				return new Tuple2<>(s, 1);
////			}
////
////		});
////
////		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
////			@Override
////			public Integer call(Integer i1, Integer i2) {
////				return i1 + i2;
////			}
////		});
////
////		wordCounts.print();
//
//		sc.start();
//		sc.awaitTermination();
//
//	}
//
//}
