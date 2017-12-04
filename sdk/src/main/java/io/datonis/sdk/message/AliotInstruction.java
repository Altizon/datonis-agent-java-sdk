package io.datonis.sdk.message;

import io.datonis.sdk.org.json.simple.JSONObject;

/**
 * @deprecated
 * 
 * Maintained for compatibility with old agents. Please use {@link Instruction}
 * 
 * @author Rajesh Jangam (rajesh_jangam@altizon.com)
 *
 */
public class AliotInstruction extends Instruction {

    public AliotInstruction(long timestamp, String alertKey, String thingKey, JSONObject instruction) {
        super(timestamp, alertKey, thingKey, instruction);
    }
    
    public AliotInstruction(Instruction i) {
        super(i.getTimestamp(), i.getAlertKey(), i.getThingKey(), i.getInstruction());
    }
}
