package com.harman.utils;

import org.json.JSONException;
import org.json.JSONObject;

import com.harman.Model.AppModel.AppAnalyticModel;
import com.harman.Model.AppModel.BoomboxAppAnalyticsModel;
import com.harman.Model.AppModel.BoomboxDeviceAnalyticsModel;
import com.harman.Model.AppModel.DeviceAnalyticModel;
import com.harman.Model.AppModel.JBLExtremeCLAnalyticsModel;
import com.harman.Model.AppModel.JBLXtremeCLDeviceAnalyticsModel;
import com.harman.models.DeviceAnalyticsModel;
import com.harman.models.HarmanDeviceModel;
import com.harman.models.PRODUCT_TYPE;

public class HarmanParser {

	public HarmanDeviceModel getParseHarmanDevice(JSONObject mHarmanDevice) throws JSONException {

		HarmanDeviceModel harmanModel = new HarmanDeviceModel();
		harmanModel.setAppVersion(mHarmanDevice.getString("AppVersion"));
		harmanModel.setFirmwareVersion(mHarmanDevice.getString("FirmwareVersion"));
		harmanModel.setColorName(mHarmanDevice.getString("colorName"));
		harmanModel.setProductName(mHarmanDevice.getString("productName"));
		harmanModel.setColorId(mHarmanDevice.getInt("colorId"));
		harmanModel.setProductId(mHarmanDevice.getInt("productId"));
		harmanModel.setMacAddress(mHarmanDevice.getString("macAddress"));

		return harmanModel;
	}

	public DeviceAnalyticsModel getParseDeviceAnalyticsModel(JSONObject mAanaytics, String macaddress)
			throws JSONException {

		DeviceAnalyticsModel deviceAnalyticsModel = new DeviceAnalyticsModel();
		String[] keys = deviceAnalyticsModel.getKeys();
		for (String key : keys) {
			if (!mAanaytics.isNull(key)) {
				deviceAnalyticsModel.getmDeviceAnaModelList().put(key, mAanaytics.getInt(key));
			}
		}
		deviceAnalyticsModel.setMacaddress(macaddress);
		/*
		 * deviceAnalyticsModel.setBroadcaster(mAanaytics.getInt("Broadcaster"))
		 * ; deviceAnalyticsModel.setReceiver(mAanaytics.getInt("Receiver"));
		 * deviceAnalyticsModel.setCriticalTemperatureShutDown(mAanaytics.getInt
		 * ("CriticalTemperatureShutDown"));
		 * deviceAnalyticsModel.setPowerBankUsage(mAanaytics.getInt(
		 * "PowerBankUsage"));
		 * deviceAnalyticsModel.setPowerOnOffCount(mAanaytics.getInt(
		 * "PowerOnOffCount"));
		 * 
		 * JSONObject mEQSettings = mAanaytics.getJSONObject("EQSettings");
		 * deviceAnalyticsModel.setEQSettings_Indoor(mEQSettings.getInt("Indoor"
		 * )); deviceAnalyticsModel.setEQSettings_Outdoor(mEQSettings.getInt(
		 * "Outdoor")); deviceAnalyticsModel.setMacaddress(macaddress);
		 */
		return deviceAnalyticsModel;

	}
	
	
	public DeviceAnalyticModel parseDeviceAnalyticsModel(JSONObject mAanaytics, String macaddress, long productId)
			throws JSONException {

		DeviceAnalyticModel deviceAnalyticsModel =null;
		if(productId == PRODUCT_TYPE.BOOMBOX) {
			deviceAnalyticsModel = new BoomboxDeviceAnalyticsModel();
		} else if(productId == PRODUCT_TYPE.JBL_XTREME_CL) {
			deviceAnalyticsModel = new JBLXtremeCLDeviceAnalyticsModel();
		}
		
		String[] keys = deviceAnalyticsModel.getKeys();
		for (String key : keys) {
			if (!mAanaytics.isNull(key)) {
				deviceAnalyticsModel.getmDeviceAnaModelList().put(key, mAanaytics.getInt(key));
			}
		}
		deviceAnalyticsModel.setMacaddress(macaddress);
		return deviceAnalyticsModel;

	}
	
	public AppAnalyticModel parseAppAnalyticsModel(JSONObject mAppAnalytics, String macaddress, long productId) throws JSONException  {
		AppAnalyticModel model = null;
		if (productId == PRODUCT_TYPE.BOOMBOX) {// Boombox
			model = new BoomboxAppAnalyticsModel();
		} else if (productId == PRODUCT_TYPE.JBL_XTREME_CL) {// JBL Xtreme2 CL
			model = new JBLExtremeCLAnalyticsModel();
		}
		String[] keys = model.getKeys();
		for (String key : keys) {
			if (!mAppAnalytics.isNull(key))
				model.getmDeviceAnaModelList().put(key, mAppAnalytics.getInt(key));
		}

		if (!mAppAnalytics.isNull("SpeakerMode")) {

			JSONObject jsonObject = mAppAnalytics.getJSONObject("SpeakerMode");

			if (!jsonObject.isNull("Stereo")) {
				model.getmDeviceAnaModelList().put("SpeakerMode_Stereo", jsonObject.getInt("Stereo"));
			}
			if (!jsonObject.isNull("Party")) {
				model.getmDeviceAnaModelList().put("SpeakerMode_Party", jsonObject.getInt("Party"));
			}
			if (!jsonObject.isNull("Single")) {
				model.getmDeviceAnaModelList().put("SpeakerMode_Single", jsonObject.getInt("Single"));
			}

		}
		model.setMacaddress(macaddress);
		
		return model;
	}

}
