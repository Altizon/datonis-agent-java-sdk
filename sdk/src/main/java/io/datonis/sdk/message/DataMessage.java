
package io.datonis.sdk.message;

import io.datonis.sdk.org.json.simple.JSONArray;
import io.datonis.sdk.org.json.simple.JSONObject;

/**
 * Represents a Data packet being transmitted to Datonis
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 *
 */
public class DataMessage extends CacheableMessage {

    private JSONObject data;
    private String thingKey;
    private Boolean isAlias;
    private JSONArray waypoint;

    public DataMessage(String thingKey, boolean isAlias, long timestamp, JSONObject data, JSONArray waypoint, Boolean isCompressed) {
        super(Message.DATA, timestamp, isCompressed);
        this.data = data;
        this.thingKey = thingKey;
        this.waypoint = waypoint;
        this.isAlias = isAlias;
    }

    public DataMessage(JSONObject json) {
        super(json);
        thingKey = (String)json.get(MessageConstants.THING_KEY);
        data = (JSONObject)json.get(MessageConstants.DATA);
        waypoint = (JSONArray) json.get(MessageConstants.WAYPOINT);
        isAlias = (Boolean)json.get(MessageConstants.IS_ALIAS);
    }

    public String getThingKey() {
        return thingKey;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject json = super.toJSON();
        if (isAlias != null && isAlias == true) {
            json.put(MessageConstants.DEVICE_KEY, thingKey);
        } else {
            json.put(MessageConstants.THING_KEY, thingKey);
        }
        if (data != null)
        	json.put(MessageConstants.DATA, data);
        if (waypoint != null)
        	json.put(MessageConstants.WAYPOINT, waypoint);
        return json;
    }

    @Override
    public String toString() {
        return toJSON().toJSONString();
    }
}
