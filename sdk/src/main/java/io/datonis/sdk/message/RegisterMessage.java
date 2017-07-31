
package io.datonis.sdk.message;

import io.datonis.sdk.Thing;

import org.json.simple.JSONObject;

/**
 * Message to register a DataStream with Datonis
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 */
public class RegisterMessage extends Message {
    private Thing thing;
    private Boolean isAlias;

    public RegisterMessage(Thing thing, boolean isAlias) {
        super(Message.THING_REGISTER, System.currentTimeMillis(), false);
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
            obj.put(MessageConstants.NAME, thing.getName());
            if (thing.getDescription() != null) {
                obj.put(MessageConstants.DESCRIPTION, thing.getDescription());
            }
            obj.put(MessageConstants.THING_KEY, thing.getKey());
        }
        obj.put(MessageConstants.BI_DIRECTIONAL, thing.isBiDirectional());

        return obj;
    }
}
