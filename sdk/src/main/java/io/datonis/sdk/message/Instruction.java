
package io.datonis.sdk.message;

import org.json.simple.JSONObject;

/**
 * Message that represents an instruction received from Datonis
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public class Instruction extends Message {
    private String thingKey;
    private String alertKey;
    private JSONObject instruction;

    public Instruction(long timestamp, String alertKey, String thingKey, JSONObject instruction) {
        super(Message.INSTRUCTION, timestamp, false);
        this.thingKey = thingKey;
        this.alertKey = alertKey;
        this.instruction = instruction;
    }

    public String getThingKey() {
        return thingKey;
    }

    public String getAlertKey() {
        return alertKey;
    }

    public JSONObject getInstruction() {
        return instruction;
    }

    public JSONObject getInstructionBody() {
        return (JSONObject)instruction.get("instruction");
    }
}
