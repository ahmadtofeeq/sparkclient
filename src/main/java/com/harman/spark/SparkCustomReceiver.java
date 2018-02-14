
package com.harman.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.json.JSONObject;

import com.harman.dbinsertion.MariaDB;
import com.harman.dbinsertion.MariadbOperator;
import com.harman.dbinsertion.MongoDBOperator;
import com.harman.models.DBkeys;

public class SparkCustomReceiver extends Receiver<String> implements DBkeys {

	/**
	* 
	*/
	private static final long serialVersionUID = 1L;

	String host = null;
	int port = -1;

	public SparkCustomReceiver(String host_, int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
	}

	final static int emailAlertCounter = 4;

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("spark://10.0.0.5:7077").setAppName("SmartAudioAnalytics")
				.set("spark.executor.memory", "1g").set("spark.cores.max", "9").set("spark.driver.cores", "2")
				.set("spark.driver.memory", "2g");
		System.out.println("1");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		JavaDStream<String> socket_one = ssc.receiverStream(new SparkCustomReceiver("52.165.145.168", 9997));
		socket_one.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<String> rdd) throws Exception {

				final long count = rdd.count();
				JavaRDD<String> filterrdd = rdd.filter(new Function<String, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {

						JSONObject jsonObject = new JSONObject();

						jsonObject = jsonObject.getJSONObject("DeviceAnalytics");
						String nextKey = jsonObject.isNull("PowerONCount") ? "PowerOnOffCount" : "PowerONCount";
						System.out.println(nextKey);
						return jsonObject.getInt(nextKey) > 5;
					}
				});

				filterrdd.foreach(new VoidFunction<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(String t) throws Exception {
						System.out.println("Entering data to Email counter");
						JSONObject jsonObject = new JSONObject();
						jsonObject = jsonObject.getJSONObject("DeviceAnalytics");
						String nextKey = jsonObject.isNull("PowerONCount") ? "PowerOnOffCount" : "PowerONCount";

						MariaDB mariadbOperator = MariaDB.getInstance();
						mariadbOperator.inserEmailCounter(jsonObject.getInt(nextKey), mariadbOperator.openConnection());
						mariadbOperator.closeConnection();
					}
				});
				rdd.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					public void call(String s) throws Exception {
						try {
							System.out.println(Thread.currentThread().getId());
						} catch (Exception e) {
							System.out.println("Unable to get thread id.");
						}
						MongoDBOperator mongoOp = MongoDBOperator.getInstance();
						mongoOp.openConnection();
						// mongoOp.updateCounter();
						mongoOp.inserSingleRecordMongoDB(s);

						MariadbOperator mariaOp = MariadbOperator.getInstance();
						mariaOp.insertIntoMariaDB(s);

						// if (mongoOp.getCounter() >= count) {
						// if (mariaOp.getFeatureCounter() > emailAlertCounter)
						// {
						// SparkTriggerThread.SendEmail("PowerOnOffCount",
						// mariaOp.getFeatureCounter());
						// }
						// mariaOp.resetFeatureCounter();
						// }
					}

				});
			}
		});
		ssc.start();
		ssc.awaitTerminationOrTimeout(Long.MAX_VALUE);
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		Thread t = new Thread() {
			@Override
			public void run() {
				receive();
			}
		};
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
		System.out.println("t1 thread priority : " + t.getPriority()); // Default
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
		System.out.println("onStop ");
	}

	// static Socket socket = null;
	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		Socket socket = null;
		String userInput = null;

		try {
			socket = new Socket(host, port);
			BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			while (!isStopped()) {
				userInput = reader.readLine();
				// System.out.println("**** Only Print - " + userInput + "\n");
				store(userInput);
			}
			System.out.println("Stream stopped");
			reader.close();
			socket.close();
			System.out.println("Trying to connect again");
		} catch (ConnectException ce) {
			// restart if could not connect to server
			System.out.println("Could not connect");
		} catch (Throwable t) {
			System.out.println("Error receiving data");
		}
	}

}
