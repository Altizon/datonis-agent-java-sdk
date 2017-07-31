package io.datonis.sdk;

import io.datonis.sdk.message.Instruction;

/**
 * Defines the way in which the instruction should be handled
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public interface InstructionHandler {
   
    /**
     * The handler method
     * 
     * @param gateway
     * @param instruction
     */
    void handleInstruction(EdgeGateway gateway, Instruction instruction);
}
