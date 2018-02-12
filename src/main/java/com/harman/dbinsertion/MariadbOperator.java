package com.harman.dbinsertion;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONException;
import org.json.JSONObject;

import com.harman.Model.AppModel.AppAnalyticModel;
import com.harman.Model.AppModel.DeviceAnalyticModel;
import com.harman.models.DBkeys;
import com.harman.models.HarmanDeviceModel;
import com.harman.utils.ErrorType;
import com.harman.utils.HarmanParser;

public class MariadbOperator extends SparkUtils implements DBkeys {

	private MariadbOperator() {

	}

	static MariadbOperator isInsertionIntoMariaDB = null;

	public static MariadbOperator getInstance() {
		if (isInsertionIntoMariaDB == null)
			isInsertionIntoMariaDB = new MariadbOperator();
		return isInsertionIntoMariaDB;
	}

	private int featureCounter = 0;

	public String insertIntoMariaDB(String record) {
		ErrorType errorType = ErrorType.NO_ERROR;
		JSONObject response = new JSONObject();
		try {
			JSONObject jsonObject = new JSONObject(record);
			MariaDB mariaModel = MariaDB.getInstance();
			HarmanParser harmanParser = new HarmanParser();
			HarmanDeviceModel deviceModel = null;
			try {
				deviceModel = harmanParser.getParseHarmanDevice(jsonObject.getJSONObject(harmanDevice));
				errorType = mariaModel.insertDeviceModel(deviceModel);
				print("Inserted into harmanDevice Error Type =" + errorType.toString());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			try {
				DeviceAnalyticModel deviceAnalyticsModel = harmanParser.parseDeviceAnalyticsModel(
						jsonObject.getJSONObject(DeviceAnalytics), deviceModel.getMacAddress(),
						deviceModel.getProductId());
				try {
					int num = deviceAnalyticsModel.getmDeviceAnaModelList().get(PowerOnOffCount) != null
							? deviceAnalyticsModel.getmDeviceAnaModelList().get(PowerOnOffCount)
							: deviceAnalyticsModel.getmDeviceAnaModelList().get("PowerONCount");
					updateFeatureCounter(num);
				} catch (Exception e) {
					print("Feature counter failed.");
				}
				errorType = mariaModel.insertDeviceAnalytics(deviceAnalyticsModel);
				print("Inserted into DeviceAnalytics Error Type =" + errorType.toString());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			try {
				AppAnalyticModel appAnalyticsModel = harmanParser.parseAppAnalyticsModel(
						jsonObject.getJSONObject(AppAnalytics), deviceModel.getMacAddress(),
						deviceModel.getProductId());
				errorType = mariaModel.insertAppAnalytics(appAnalyticsModel);
				print("Inserted into AppAnalytics Error Type =" + errorType.toString());
			} catch (JSONException e) {
				errorType = ErrorType.INVALID_JSON;
			}

			switch (errorType) {
			case NO_ERROR:
				response.put("Status", 1);
				break;

			default:
				response.put("Status", 0);
				break;
			}
			response.put("cmd", "UpdateSmartAudioAnalyticsRes");
		} catch (Exception e) {
			response.put("Status", 0);
			response.put("cmd", "UpdateSmartAudioAnalyticsRes");
		} finally {
			MariaDB.getInstance().closeConnection();
		}
		print(errorType.name());
		return response.toString();
	}

	public int getFeatureCounter() {
		return featureCounter;
	}

	public void updateFeatureCounter(int featureCounter) {
		featureCounter += featureCounter;
	}

	public void resetFeatureCounter() {
		featureCounter = 0;
	}

	public void putPerIntervalCriticalTempShutdown(long count) {
		ErrorType response = ErrorType.NO_ERROR;
		MariaDB mariaModel = MariaDB.getInstance();
		Connection connection = mariaModel.openConnection();
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			try {

				String query = "INSERT INTO BatchCriticalTempShutDownTable (CriticalTempshutDown) VALUE(" + count + ")";
				System.out.println(query);
				int result = stmt.executeUpdate(query);
				if (result == 0)
					response = ErrorType.ERROR_INSERTING_DB;
				else {
					System.out.println("Critical Temperature shutdown count inserted to DB");
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_INSERTING_DB;
			}
		} catch (Exception e) {
			response = ErrorType.NETWORK_NOT_AVAILBLE;
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_CLOSING_DB;
				System.out.println("SQLException while closing data");
			}

			mariaModel.closeConnection();

		}

		System.out.println("Response is : " + response);
	}

}