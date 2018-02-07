package com.harman.spark;

import java.io.Serializable;

import com.harman.dbinsertion.MariaDB;

public class SparkTriggerThread implements Runnable, Serializable {

	private String feature;
	private int value;

	private SparkTriggerThread(String feature, int value) {
		this.feature = feature;
		this.value = value;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void SendEmail(String feature, int value) {

		new Thread(new SparkTriggerThread(feature, value)).start();

	}

	public void run() {

		MariaDB mariadbOperator = MariaDB.getInstance();
		mariadbOperator.inserEmailCounter(value, mariadbOperator.openConnection());
		mariadbOperator.closeConnection();
		System.out.println("****ALERT " + feature + " is need to be investigated !!!!!!!!!!!!!!!!!!");
	}

}
