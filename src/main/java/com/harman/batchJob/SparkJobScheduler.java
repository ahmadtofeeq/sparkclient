package com.harman.batchJob;
import java.util.Date;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;


public class SparkJobScheduler {

	private static SparkJobScheduler sjs = null;
	private static Scheduler batch_sched;

	public static SparkJobScheduler getInstance() {
		if (sjs == null)
			sjs = new SparkJobScheduler();
		return sjs;
	}

	public void mSparkJobScheduler(/*JavaSparkContext gContext*/)
	{
		try {
			/* create a scheduler */
			SchedulerFactory sf = new StdSchedulerFactory();
			batch_sched = sf.getScheduler();

		}catch(Exception e)
		{
			e.printStackTrace();
		}


		JobDetail job1 = JobBuilder.newJob(BatchAnalyticJob.class)
				.withIdentity("batchAnalyticJob", "group1")
				.build();

		//This trigger will run every minute in infinite loop
		System.out.println("Job build done");
		Trigger trigger1 = TriggerBuilder.newTrigger()
				.withIdentity("trigger1", "group1")
				.startNow()
				.withSchedule(SimpleScheduleBuilder.simpleSchedule()
						.withIntervalInMinutes(1)
						.repeatForever())
				.build();

		System.out.println("Job trigger done");
		try {

			Date ft = batch_sched.scheduleJob(job1, trigger1);
			batch_sched.start();

			System.out.println(job1.getKey() + " has been scheduled to run at: " + ft);
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			System.out.println("SchedulerException in catch block");
			e.printStackTrace();
		}
	}
}

