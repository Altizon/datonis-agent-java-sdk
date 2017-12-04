package io.datonis.sdk.message;

import io.datonis.sdk.org.json.simple.JSONObject;

/**
 * Bean that represents an alert message sent to Datonis 
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public class AlertMessage extends CacheableMessage {
    private String alertKey;
    private String thingKey;
    private AlertType alertType;
    private String message;
    private JSONObject data;
    private Boolean isAlias;

    public AlertMessage(String alertKey, String thingKey, boolean isAlias, AlertType alertType, String message, JSONObject data) {
        super(Message.ALERT, System.currentTimeMillis(), false);
        this.alertKey = alertKey;
        this.thingKey = thingKey;
        this.alertType = alertType;
        this.message = message;
        this.data = data;
        this.isAlias = isAlias;
    }
    
    public AlertMessage(JSONObject json) {
        super(json);
        json = (JSONObject)json.get(MessageConstants.ALERT);
        alertKey = (String)json.get(MessageConstants.ALERT_KEY);
        isAlias = (Boolean)json.get(MessageConstants.IS_ALIAS);
        thingKey = (String)json.get(MessageConstants.THING_KEY);
        alertType = AlertType.values()[(Integer)json.get(MessageConstants.ALERT_TYPE)];
        message = (String)json.get(MessageConstants.MESSAGE);
        data = (JSONObject)json.get(MessageConstants.DATA);
    }
    
    public String getKey() {
        return this.alertKey;
    }
    
    public JSONObject toJSON() {
        JSONObject wrapper = new JSONObject();
        
        JSONObject obj = super.toJSON();
        if (alertKey != null) {
            obj.put(MessageConstants.ALERT_KEY, alertKey);
        }
        if (isAlias != null && isAlias == true) {
            obj.put(MessageConstants.DEVICE_KEY, thingKey);
        } else {
            obj.put(MessageConstants.THING_KEY, thingKey);
        }

        obj.put(MessageConstants.ALERT_TYPE, alertType.ordinal());
        obj.put(MessageConstants.MESSAGE, message);
        if (data != null) {
            obj.put(MessageConstants.DATA, data);
        }
        wrapper.put(MessageConstants.ALERT, obj);
        return wrapper;
    }
}
