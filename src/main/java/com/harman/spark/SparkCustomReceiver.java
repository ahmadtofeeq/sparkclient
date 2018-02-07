
package com.harman.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.harman.dbinsertion.MongoDBOperator;
import com.harman.dbinsertion.MariadbOperator;
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
		System.out.println("52.165.145.168");
		SparkConf sparkConf = new SparkConf().setMaster("spark://10.0.0.5:7077").setAppName("SmartAudioAnalytics")
				.set("spark.executor.memory", "1g").set("spark.cores.max", "5").set("spark.driver.cores", "2")
				.set("spark.driver.memory", "2g");
		System.out.println("1");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(3000));
		JavaDStream<String> socket_one = ssc.receiverStream(new SparkCustomReceiver("52.165.145.168", 9997));
		JavaDStream<String> socket_two = ssc.receiverStream(new SparkCustomReceiver("52.165.145.168", 9997));

		ArrayList<JavaDStream<String>> streamList = new ArrayList<JavaDStream<String>>();
		streamList.add(socket_two);
		JavaDStream<String> UnionStream = ssc.union(socket_one, streamList);

		UnionStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<String> rdd) throws Exception {

				final long count = rdd.count();

				rdd.foreach(new VoidFunction<String>() {

					private static final long serialVersionUID = 1L;

					public void call(String s) throws Exception {
						MongoDBOperator mongoOp = MongoDBOperator.getInstance();
						mongoOp.openConnection();
						mongoOp.updateCounter();
						mongoOp.inserSingleRecordMongoDB(s);

						MariadbOperator mariaOp = MariadbOperator.getInstance();
						mariaOp.insertIntoMariaDB(s);

						if (mongoOp.getCounter() >= count) {
							if (mariaOp.getFeatureCounter() > emailAlertCounter) {
								SparkTriggerThread.SendEmail("PowerOnOffCount", mariaOp.getFeatureCounter());
							}
							mariaOp.resetFeatureCounter();
						}
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
				System.out.println("**** Only Print - " + userInput + "\n");
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
