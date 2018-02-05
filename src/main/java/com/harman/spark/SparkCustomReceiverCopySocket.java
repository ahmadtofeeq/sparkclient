package com.harman.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.harman.models.DBkeys;

public class SparkCustomReceiverCopySocket extends Receiver<String> implements DBkeys {

	/**
	* 
	*/
	private static final long serialVersionUID = 1L;

	String host = null;
	int port = -1;

	public SparkCustomReceiverCopySocket(String host_, int port_) {
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
		JavaDStream<String> lines = ssc.receiverStream(new SparkCustomReceiverCopySocket("52.165.145.168", 9997));
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {

				if (rdd.count() == 0) {
					System.out.println("RDD count is 0");
				} else
					System.out.println("RDD count is >0");

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
		ssc.awaitTerminationOrTimeout(Long.MAX_VALUE);
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		new Thread() {
			@Override
			public void run() {
				try {
					Socket socket = new Socket(host, port);
					receive(socket);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}.start();
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false

		System.out.println("onStop ");
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive(Socket socket) {
		String userInput = null;

		try {
			// connect to the server
			socket = new Socket(host, port);
			BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			// Until stopped or connection broken continue reading
			while (!isStopped()) {
				System.out.println("Trying to fetch data ");
				userInput = reader.readLine();
				System.out.println("Received data '" + userInput + "'");
				store(userInput);
			}
			System.out.println("stream stopped");
			reader.close();
			socket.close();
			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}

}
