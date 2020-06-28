package com.stephen.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * create SparkContext and SparkSession
 * @author qwang
 *
 */
public class SparkBuilder {
	private static SparkSession sparkSession = null;
	private static JavaSparkContext sparkContext = null;
	
	/**
	 * instantiate Singleton SparkContext
	 */
	private static void initializeSparkContext() {
		if (sparkContext == null) {
					SparkConf conf = new SparkConf()
							.setAppName(SparkConfigure.SPARK_NAME)
							.setMaster(SparkConfigure.SPARK_MASTER)
							.set("spark.logConf", "false");
	
					System.setProperty("hadoop.home.dir", SparkConfigure.HADOOP_HOME);	
					sparkContext = new JavaSparkContext(conf);
					sparkContext.setLogLevel("ERROR");
		}
	}

	/**
	 * instantiate Singleton SparkSession
	 */
	private static void initializeSparkSession() {
		if (sparkSession == null) {
					System.setProperty("hadoop.home.dir", SparkConfigure.HADOOP_HOME);	
					sparkSession = SparkSession
							  .builder()
							  .appName(SparkConfigure.SPARK_NAME)
							  .master(SparkConfigure.SPARK_MASTER)
							  .config("spark.sql.warehouse.dir", SparkConfigure.SPARK_TEMP)
							  .getOrCreate();
		}
	}
	
	public static JavaSparkContext createJavaSparkContext() {
		initializeSparkContext();
		return sparkContext;
	}
	
	public static SparkSession createSparkSession() {
		initializeSparkContext();
		initializeSparkSession();
		return sparkSession;
	}
	
	
}
