package com.yildiz.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class _010_FromFileReadTextToRDD2 {

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf()
        		.setAppName("Read Multiple Text Files to RDD")
                .setMaster("local[2]")
                .set("spark.executor.memory", "2g");

        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // provide path to directory containing text files
        String files = "data/rdd/input";

        // read text files to RDD
        JavaRDD<String> lines = sc.textFile(files);

        // collect RDD for printing
        for (String line : lines.collect()) {
            System.out.println(line);
        }
    }
}