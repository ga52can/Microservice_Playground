package anomalyDetection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class SimpleApp {

	public static void main(String[] args) {
		
		
		String logFile = "C:/spark/README.md"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("Simple Application")
	    								.setMaster("local[4]")
	    								.set("spark.local.dir", "C:/tmp");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.setLevel(Level.ERROR);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();

	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    
	    sc.stop();

		
	}

}
