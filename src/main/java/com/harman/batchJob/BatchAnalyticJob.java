package com.harman.batchJob;

import java.io.Serializable;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.AccumulatorV2;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.harman.dbinsertion.MariadbOperator;
import com.mongodb.spark.MongoSpark;

// This a job which we would be scheduling in cscheduler
@SuppressWarnings("serial")
public class BatchAnalyticJob implements Job, Serializable {

	public BatchAnalyticJob() {

	}

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {

			// Load data and infer schema, disregard toDF() name as it returns
			// Dataset
			Dataset<Row> fullJsonReqDS = MongoSpark.load(SparkBatchJob.global_context).toDF();
			fullJsonReqDS.createOrReplaceTempView("implicitDS");
			System.out.println("Full Count=" + fullJsonReqDS.count());

			// Get the value with timefileter and create view
			Dataset<Row> dataOnTimeFilter = SparkBatchJob.global_spark_session.sql(
					"select DeviceAnalytics.CriticalTemperatureShutDown from implicitDS where date > NOW() - INTERVAL 120 HOUR");
			dataOnTimeFilter.createOrReplaceTempView("dataOnTimeFilter");
			System.out.println("after time filter Count=" + dataOnTimeFilter.count());
			dataOnTimeFilter.printSchema();
			dataOnTimeFilter.show(1, false);

			/*
			 * Dataset<Row> element= dataOnTimeFilter.select(
			 * "DeviceAnalytics.CriticalTemperatureShutDown","date").toDF();
			 * element.createOrReplaceTempView("element");
			 * element.show(3,false);
			 */

			// Accumulator<Integer> accum =
			// SparkBatchJob.global_context.intAccumulator(0);
			AccumulatorV2<Long, Long> accum2 = SparkBatchJob.global_context.sc().longAccumulator();
			System.out.println("Before foreach call");

			dataOnTimeFilter.foreach(new ForeachFunction<Row>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Row temp) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("[Shivam] Under foeach i should be in executor");
					long value = temp.getInt(0);
					System.out.println("[Shivam] value of criticaltempshutdown row is =" + value);
					if (value > 4) {
						accum2.add(value);
					}
				}

			});

			System.out.println("end of batch");

			MariadbOperator insertIntoMaria = MariadbOperator.getInstance();
			insertIntoMaria.putPerIntervalCriticalTempShutdown(accum2.value());

		} catch (Exception e) {
			System.out.println("Exception in execute function of scheduler");
			e.printStackTrace();
			// TODO: handle exception
		}

	}
}
