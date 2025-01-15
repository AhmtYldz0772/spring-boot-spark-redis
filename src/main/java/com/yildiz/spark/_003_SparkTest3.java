package com.yildiz.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class _003_SparkTest3 {

	public static void main(String[] args) {
		//	SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[1]");
		//	JavaSparkContext sc = new JavaSparkContext(conf);
		JavaSparkContext sc = new JavaSparkContext("local", "Count files");

		// String directory = System.getProperty("user.home");
		// String fileName = "data/rdd/input/mysample.txt";
		// String absolutePath = directory + File.separator + fileName;

		JavaRDD<String> textFileRDD = sc.textFile("data/rdd/input/mysample.txt");

		System.out.println("Count:" + textFileRDD.count());
		sc.close();
	}
}