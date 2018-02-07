package com.harman.Model.AppModel;

import com.harman.models.PRODUCT_TYPE;

public class BoomboxDeviceAnalyticsModel extends DeviceAnalyticModel {
	@Override
	public long getDeviceType() {
		return PRODUCT_TYPE.BOOMBOX;
	}

	@Override
	public String[] getKeys() {
		return new String[] { "JBLConnectUsage", "JBLConnectDuration", "Broadcaster", "Receiver",
				"CriticalTemperatureShutDown", "PowerOnOffCount", "PowerOnOffDuration", "EQSettings", "PowerBankUsage",
				"MusicPlaybackHrs", "MusicPlaybackOnBattery", "Speakerphone", "ChargingTime" };
	}
}
