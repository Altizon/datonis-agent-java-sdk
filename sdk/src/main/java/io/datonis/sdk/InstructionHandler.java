package io.datonis.sdk;

import io.datonis.sdk.message.AliotInstruction;
import io.datonis.sdk.message.Instruction;


/**
 * Defines the way in which the instruction should be handled
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public abstract class InstructionHandler {
   
    /**
     * The handler method for an instruction.
     * Here is where you write code for actuating downstream
     * 
     * @param gateway
     * @param instruction
     * @return 
     * 
     */
    public void handleInstruction(EdgeGateway gateway, Instruction instruction) {
        
    }

    /**
     * @deprecated
     * 
     * Maintained for compatibility reasons
     * Please override the {@link InstructionHandler#handleInstruction(EdgeGateway, Instruction)} instead
     * 
     * @param gateway
     * @param instruction
     */
    public void handleInstruction(AliotGateway gateway, AliotInstruction instruction) {
        
    }
}
