
package io.datonis.sdk.message;

import io.datonis.sdk.org.json.simple.JSONObject;

/**
 * Represents a message to be transmitted to Datonis
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 *
 */
public abstract class Message {
    public static final String THING_REGISTER = "THING_REGISTER";
    public static final String THING_HEARTBEAT = "THING_HEARTBEAT";
    public static final String DATA = "DATA";
    public static final String BULKDATA = "BULKDATA";
    public static final String ALERT = "ALERT";
    public static final String INSTRUCTION = "INSTRUCTION";

    private String type;
    private long timestamp;
    private volatile int transmitStatus;
    private String accessKey;
    private String secretKey;
    private Boolean isCompressed;

    public Message(String type, long timestamp, Boolean isCompressed) {
        this.type = type;
        this.timestamp = timestamp;
        this.isCompressed = isCompressed;
    }

    public Message(JSONObject obj) {
        this.type = (String)obj.get(MessageConstants.TYPE);
        this.timestamp = (Long)obj.get(MessageConstants.TIMESTAMP);
        this.isCompressed = (Boolean)obj.get(MessageConstants.COMPRESSED);
        if (this.isCompressed == null) {
        	this.isCompressed = false;
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getType() {
        return type;
    }

    public int getTransmitStatus() {
        return transmitStatus;
    }

    public void setTransmitStatus(int transmitStatus) {
        this.transmitStatus = transmitStatus;
    }
    
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }
    
    public String getAccessKey() {
        return this.accessKey;
    }
    
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
    
    public String getSecretKey() {
        return this.secretKey;
    }
    
    public JSONObject toJSON() {
        JSONObject obj = new JSONObject();
        obj.put(MessageConstants.TIMESTAMP, timestamp);
        obj.put(MessageConstants.TYPE, type);
        obj.put(MessageConstants.COMPRESSED, isCompressed);
        return obj;
    }
    
    public JSONObject toJSON(boolean skipUnwanted) {
        JSONObject obj = this.toJSON();
        if (skipUnwanted) {
            obj.remove(MessageConstants.TYPE);
            obj.remove(MessageConstants.COMPRESSED);
        }
        return obj;
    }

	public Boolean getIsCompressed() {
		return isCompressed;
	}

	public void setIsCompressed(Boolean isCompressed) {
		this.isCompressed = isCompressed;
	}
}
