package com.harman.batchJob;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import com.harman.batchJob.SparkJobScheduler;



public final class SparkBatchJob {
	public static JavaSparkContext global_context;
	public static SparkSession global_spark_session;

	public static void main(final String[] args) throws InterruptedException {

		global_spark_session = SparkSession.builder()
				.master("spark://10.0.0.5:7077")
				.appName("BatchAnalyticsApp")
				.config("spark.executor.memory", "1g")
				.config("spark.cores.max", "3")
				.config("spark.mongodb.input.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.config("spark.mongodb.output.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.getOrCreate();

		// get Context
		global_context = new JavaSparkContext(global_spark_session.sparkContext());

		//Invoke quartz scheduler to schedule job
		SparkJobScheduler schedule = SparkJobScheduler.getInstance();
		schedule.mSparkJobScheduler();

	}

}