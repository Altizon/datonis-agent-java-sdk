
package io.datonis.sdk.message;

import io.datonis.sdk.Thing;

import org.json.simple.JSONObject;

/**
 * Heartbeat message
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 *
 */
public class HeartbeatMessage extends Message {
    private Thing thing;
    private Boolean isAlias;

    public HeartbeatMessage(Thing thing, boolean isAlias, long timestamp) {
        super(Message.THING_HEARTBEAT, timestamp, false);
        this.thing = thing;
        this.isAlias = isAlias;
        this.setAccessKey(thing.getAccessKey());
        this.setSecretKey(thing.getSecretKey());
    }

    public Thing getThing() {
        return thing;
    }

    public JSONObject toJSON() {
        JSONObject obj = super.toJSON();
        if (isAlias != null && isAlias) {
            obj.put(MessageConstants.DEVICE_KEY, thing.getKey());
        } else {
            obj.put(MessageConstants.THING_KEY, thing.getKey());
        }
        return obj;
    }
}
