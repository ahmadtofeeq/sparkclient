package com.harman.utils;

import org.apache.log4j.Logger;

public class HarmanUtils {

	
	//}

	public static Logger returnLogObject(Object o) {

		Logger logger = Logger.getLogger(o.getClass());
		
		return logger;

	}
}
