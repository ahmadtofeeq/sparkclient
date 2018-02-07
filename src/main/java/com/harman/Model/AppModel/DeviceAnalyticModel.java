package com.harman.Model.AppModel;

import java.util.LinkedHashMap;

public abstract class DeviceAnalyticModel {
	private String macaddress;
	private LinkedHashMap<String, Integer> mDeviceAnaModelList = new LinkedHashMap<String, Integer>();

	public String getMacaddress() {
		return macaddress;
	}

	public void setMacaddress(String macaddress) {
		this.macaddress = macaddress;
	}

	public LinkedHashMap<String, Integer> getmDeviceAnaModelList() {
		return mDeviceAnaModelList;
	}

	public void setmDeviceAnaModelList(LinkedHashMap<String, Integer> mDeviceAnaModelList) {
		this.mDeviceAnaModelList = mDeviceAnaModelList;
	}

	public abstract long getDeviceType();

	public abstract String[] getKeys();

}
