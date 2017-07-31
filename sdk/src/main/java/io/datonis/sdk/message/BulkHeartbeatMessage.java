
package io.datonis.sdk.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import io.datonis.sdk.Thing;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Heartbeat message
 * 
 * @author Pooja Karande (pooja_karande@altizon.com)
 *
 */
public class BulkHeartbeatMessage extends Message {
    private ArrayList<Thing> things;
    private Boolean isAlias;

    public BulkHeartbeatMessage(ArrayList<Thing> things, long timestamp) {
        super(Message.THING_HEARTBEAT, timestamp, false);
        this.things = things;
        this.setAccessKey(things.get(0).getAccessKey());
        this.setSecretKey(things.get(0).getSecretKey());
    }

    public JSONObject toJSON() {
    	JSONObject ret = new JSONObject();
    	JSONArray arr = new JSONArray();
    	for (int i = 0; i < things.size(); i++) {
    		JSONObject obj = super.toJSON();
            obj.put(MessageConstants.THING_KEY, things.get(i).getKey());
    		arr.add(obj);
		}
        ret.put(MessageConstants.EVENTS, arr);
        return ret;
    }
}
