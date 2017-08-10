package io.datonis.sdk;

import io.datonis.sdk.message.AliotInstruction;
import io.datonis.sdk.message.Instruction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Defines the way in which the instruction should be handled
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public abstract class InstructionHandler {
    private static Logger logger = LoggerFactory.getLogger(InstructionHandler.class);
   
    /**
     * The handler method for an instruction.
     * Here is where you write code for actuating downstream
     * 
     * @param gateway
     * @param instruction
     * @return 
     * 
     */
    public void handleInstructionExecution(EdgeGateway gateway, Instruction instruction) {
        if (gateway instanceof AliotGateway) {
            handleInstruction((AliotGateway)gateway, new AliotInstruction(instruction));
        } else {
            logger.info("Instruction received: " + instruction.getInstruction().toJSONString());
            logger.info("Please override 'handleInstructionExecution' method for handling");
        }
    }

    /**
     * @deprecated
     * 
     * Maintained for compatibility reasons
     * Please override the {@link InstructionHandler#handleInstructionExecution(EdgeGateway, Instruction)} instead
     * 
     * @param gateway
     * @param instruction
     */
    public void handleInstruction(AliotGateway gateway, AliotInstruction instruction) {
        logger.info("Instruction received: " + instruction.getInstruction().toJSONString());
        logger.info("Please override 'handleInstructionExecution' method");
    }
}
