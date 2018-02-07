package com.harman.Model.AppModel;

import com.harman.models.PRODUCT_TYPE;

public class JBLXtremeCLDeviceAnalyticsModel extends DeviceAnalyticModel {

	@Override
	public long getDeviceType() {
		return PRODUCT_TYPE.JBL_XTREME_CL;
	}

	@Override
	public String[] getKeys() {
		return new String[] { "DurationJBLConnect", "PlaytimeInBattery", "Speakerphone", "DurationPowerONOFF",
				"PowerBank", "CriticalTemperature", "ChargingTime", "PowerONCount", "JBLConnect", "Playtime" };
	}

}
