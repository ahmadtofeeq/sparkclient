package com.harman.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.harman.models.DBkeys;

public class SparkClientSingleReceiver implements DBkeys {

	final static int emailAlertCounter = 4;
	@SuppressWarnings("unused")
	private JavaStreamingContext ssc = null;

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.out.println("52.165.145.168");
		SparkConf sparkConf = new SparkConf().setMaster("spark://10.0.0.5:7077").setAppName("SmartAudioAnalytics")
				.set("spark.executor.memory", "1g").set("spark.cores.max", "5").set("spark.driver.cores", "2")
				.set("spark.driver.memory", "2g");
		System.out.println("1");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(3000));

		JavaReceiverInputDStream<String> JsonReq1 = ssc.socketTextStream("52.165.145.168", 9997,
				StorageLevels.MEMORY_AND_DISK_SER);

		JsonReq1.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {

				System.out.println("driver");
				
				rdd.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(String s) throws Exception {
						System.out.println(s);
					}

				});
			}
		});
		ssc.start();
		ssc.awaitTermination();
	}

}
