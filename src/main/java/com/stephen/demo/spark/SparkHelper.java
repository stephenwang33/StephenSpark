package com.stephen.demo.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHelper {
	
	private static JavaSparkContext context = SparkBuilder.createJavaSparkContext();
	private static SparkSession spark = SparkBuilder.createSparkSession();

	public static void setLoggerLevel(Level level) {
		Logger.getLogger("org").setLevel(level);
		Logger.getLogger("akka").setLevel(level);
	}
	
	/**
	 * generate the spark dataset for computing
	 * @param url
	 * @param file
	 * @param isReplacingFile
	 * @return
	 */
	public static Dataset<Row> loadJSonUrlData(String url, String file, boolean isReplacingFile) {
		generateUrlContext(url, file, isReplacingFile);
		return readDatasetByJSon(file);
	}
	
	/**
	 * load json file for spark computing
	 * @param path
	 * @return
	 */
	private static Dataset<Row> readDatasetByJSon(String path) {
		return spark.read().json(path).toDF();
	}
	
	/**
	 * load the external file for spark computing
	 * @param path
	 * @param fileFormat  such as "json"
	 * @return
	 */
	public static Dataset<Row> loadDatasetByJSon(String path, String fileFormat) {
		return spark.read().format(fileFormat).load(path).toDF();
	}

	/**
	 * retrieve remote data and save it into the file
	 *  	
	 * @param path. the remote URL
	 * @param file  the file name with the path such as "src\\main\\resources\\data\\products.json"
	 */
	private static void generateUrlContext(String path, String file, boolean isReplacingExistFile) {
		BufferedReader in = null;
		BufferedWriter bw = null;
		try {
			URL url = new URL(path);
			in = new BufferedReader(
			        new InputStreamReader(url.openStream()));

			File out = new File(file);
			if (out.exists()) {
				if (isReplacingExistFile) {
					out.delete();
				} else return;
			}
			
			bw = new BufferedWriter(new FileWriter(out));
			
			String inputLine;
	        while ((inputLine = in.readLine()) != null) {
	            bw.write(inputLine);
	        }
	        bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
		        bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
	
	public static JavaRDD<String> LoadCSVData(String path) {
		return context.textFile(path);
	}

}
