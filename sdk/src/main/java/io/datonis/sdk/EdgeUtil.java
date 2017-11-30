package io.datonis.sdk;

import io.datonis.sdk.communicator.EdgeCommunicator;

import java.net.HttpURLConnection;

/**
 * Provides useful functions required during communication with Datonis 
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public class EdgeUtil {
	public static int convertHTTPResponseCode(int responsecode)
    {
        int retval = 0;

        switch (responsecode) {
        case HttpURLConnection.HTTP_ACCEPTED:
        case HttpURLConnection.HTTP_OK:
            retval = EdgeCommunicator.OK;
            break;
        case HttpURLConnection.HTTP_UNAUTHORIZED:
        	retval = EdgeCommunicator.UNAUTHORIZED;
        	break;
    	case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
            // 422 stands for unprocessable entity and is thrown when when data is sent in a format that is not
            // acceptable to the datonis server.
            retval = EdgeCommunicator.NOT_ACCEPTABLE;
            break;
    	case 422:
    		retval = EdgeCommunicator.INVALID_REQUEST;
    		break;
    	case 429:
            // Thrown when the agent is sending data at a rate that is unacceptable to the appserver and needs to
            // throttle down.
    	case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
    	case 503:
            retval = EdgeCommunicator.EXCESSIVE_RATE;
        	break;
    	case 408:
    	    retval = EdgeCommunicator.TIMED_OUT;
    	    break;
    	case 500:
    	    retval = EdgeCommunicator.INTERNAL_SERVER_ERROR;
    	    break;
    	case 400:
    	    retval = EdgeCommunicator.BAD_REQUEST;
    	    break;
        default:
            retval = responsecode;
        	break;
        }
        
        return retval;
    }
	
    public static String getMappedErrorMessage(int code) {
        switch (code) {
        case EdgeCommunicator.UNAUTHORIZED:
            return "Unauthorized access. Please check your access and secret key";

        case EdgeCommunicator.EXCESSIVE_RATE:
            return "You are pushing data at a rate that is greater than what your license allows";

        case EdgeCommunicator.INVALID_REQUEST:
            return "Request is invalid";

        case EdgeCommunicator.FAILED:
            return "Failed to send data. Reasons are unknown";

        case EdgeCommunicator.NOT_ACCEPTABLE:
            return "Failed to send data. Request is unacceptable";

        case EdgeCommunicator.NO_CONNECTION:
            return "Not connected";

        case EdgeCommunicator.TIMED_OUT:
            return "Could not get response back from the server in a timely manner - timed out";
            
        case EdgeCommunicator.BAD_REQUEST:
            return "Could not process request. Please check if all parameters have been supplied correctly.";

        case EdgeCommunicator.INTERNAL_SERVER_ERROR:
            return "Server encountered an unexpected error. Please try again. If problem persists, please contact Altizon support";
       
        default:
            return "Failed to send data. The response code is: " + code;
        }
    }

	public static String getDataUrl(String datonisBaseUrl) {
	    return datonisBaseUrl + "/api/v3/things/event.json";
	}
	
	public static String getBulkDataUrl(String datonisBaseUrl) {
        return datonisBaseUrl + "/api/v3/things/event.json";
	}
	
	public static String getHeartBeatUrl(String datonisBaseUrl) {
        return datonisBaseUrl + "/api/v3/things/heartbeat.json";
	}
	
	public static String getRegisterUrl(String datonisBaseUrl) {
        return datonisBaseUrl + "/api/v3/things/register.json";
	}
	
	public static String getCreateAlertUrl(String datonisBaseUrl) {
	    return datonisBaseUrl + "/api/v3/alerts.json";
	}

	public static String getLegacyCreateAlertUrl(String datonisBaseUrl) {
        return datonisBaseUrl + "/api/v2/sensor_alerts.json";
    }

	public static String getUpdateAlertUrl(String datonisBaseUrl, String alertKey) {
	    return datonisBaseUrl + "/api/v3/alerts/" + alertKey;
	}

	public static String getLegacyUpdateAlertUrl(String datonisBaseUrl, String alertKey) {
        return datonisBaseUrl + "/api/v2/sensor_alerts/" + alertKey;
	}
	
    public static String getRuleNotificationUrl(String datonisBaseUrl) {
        return datonisBaseUrl + "/api/v3/things/rule_notification";
    }

    public static String getDownloadFileUrl(String datonisBaseUrl) {
        return datonisBaseUrl + "/api/v3/files/download_file";
    }
    
    public static String getRegisterGatewayUrl(String datonisBaseUrl) {
    	return datonisBaseUrl + "/api/v3/gateway/register";
    }

	public static String getMqttDataTopic(String clientId) {
	    return "Altizon/Datonis/" + clientId + "/event";
	}
	
	public static String getMqttBulkDataTopic(String clientId) {
	    return "Altizon/Datonis/" + clientId + "/event";
	}

	public static String getMqttCompressedDataTopic(String clientId) {
	    return "Altizon/Datonis/lzf/" + clientId + "/event";
	}

	public static String getMqttCompressedBulkDataTopic(String clientId) {
	    return "Altizon/Datonis/lzf/" + clientId + "/event";
	}

	public static String getMqttHeartBeatTopic(String clientId) {
	    return "Altizon/Datonis/" + clientId + "/heartbeat";
	}

	public static String getMqttRegisterTopic(String clientId) {
	    return "Altizon/Datonis/" + clientId + "/register";
	}

	public static String getMqttHttpAckTopic(String clientId) {
	    return "Altizon/Datonis/" + clientId + "/httpAck";
	}
	
	public static String getMqttInstructionTopic(String accessKey, String thingKey) {
	    return "Altizon/Datonis/" + accessKey + "/thing/" + thingKey + "/executeInstruction";
	}
	
	public static String getLegacyMqttInstructionTopic(String accessKey, String thingKey) {
	    return "Altizon/Datonis/" + accessKey + "/sensor/" + thingKey + "/executeInstruction";
	}
	
	public static String getMqttGatewayInstructionTopic(String accessKey, String gatewayKey) {
        return "Altizon/Datonis/" + accessKey + "/gateway/" + gatewayKey + "/executeInstruction";
    }
	
	public static String getMqttAlertTopic(String clientId) {
	    return "Altizon/Datonis/" + clientId + "/alert";
	}
 
	public static String getMqttRuleNotificationTopic(String clientId) {
        return "Altizon/Datonis/" + clientId + "/rule_notification";
    }
	
	public static String getMqttRegisterGatewayTopic(String clientId) {
        return "Altizon/Datonis/" + clientId + "/register_gateway";
    }

	public static String getMqttUpdateNotificationTopic(String accessKey) {
	    return "Altizon/Datonis/" + accessKey + "/updateNotifications";
	}
}
