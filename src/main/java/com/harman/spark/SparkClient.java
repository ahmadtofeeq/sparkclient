package com.harman.spark;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.harman.dbinsertion.InsertIntoMongoDB;
import com.harman.dbinsertion.InsertionIntoMariaDB;
import com.harman.models.DBkeys;

public class SparkClient implements DBkeys {

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

		JavaDStream<String> JsonReq1 = ssc.socketTextStream("52.165.145.168", 9997, StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> JsonReq2 = ssc.socketTextStream("52.165.145.168", 9997, StorageLevels.MEMORY_AND_DISK_SER);
		ArrayList<JavaDStream<String>> streamList = new ArrayList<JavaDStream<String>>();
		streamList.add(JsonReq1);
		JavaDStream<String> UnionStream = ssc.union(JsonReq2, streamList);

		UnionStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;
			int total = 0;

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {

				long count = rdd.count();
				total += count;
				System.out.println(total);
				rdd.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(String s) throws Exception {
						System.out.println(s);
						InsertIntoMongoDB insertMongo = InsertIntoMongoDB.getInstance();
						insertMongo.openConnection();
						insertMongo.updateCounter();
						insertMongo.inserSingleRecordMongoDB(s);

						InsertionIntoMariaDB insertMaria = InsertionIntoMariaDB.getInstance();
						insertMaria.insertIntoMariaDB(s);

						if (insertMongo.getCounter() >= count) {
							if (insertMaria.getFeatureCounter() > emailAlertCounter) {
								// send email
								SparkTriggerThread.SendEmail("CriticalTemperatureShutDown",
										insertMaria.getFeatureCounter());
							}
							insertMaria.resetFeatureCounter();
						}

					}

				});
			}
		});
		ssc.start();
		ssc.awaitTerminationOrTimeout(Long.MAX_VALUE);
	}

}
