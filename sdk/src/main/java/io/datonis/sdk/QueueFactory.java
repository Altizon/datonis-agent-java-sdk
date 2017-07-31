package io.datonis.sdk;

import io.datonis.sdk.communicator.EdgeCommunicator;
import io.datonis.sdk.message.CacheableMessage;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Queue factory that provides queue instances for the gateway
 * Implementations are Open to override and provide their own (persistence or non-persistence based) implementations
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public class QueueFactory {
    
    /**
     * Creates a blocking queue 
     * @param capacity
     * @param name
     * @return
     */
    public <T extends CacheableMessage> BlockingQueue<T> createQueue(int capacity, String name, Class<T> clazz) {
        return new LinkedBlockingQueue<>(capacity);
    }

    /**
     * Callback for cleaning up specified message from persistent store when it is no longer necessary
     * 
     * @param q
     * @param messages
     */
    public <T extends CacheableMessage> void cleanUpCallback(BlockingQueue<T> q, Collection<T> messages) {
        // A callback for persistent queue implementations
        // that allows them to cleanup the specified messages from the persistent queue
    }
    
    public <T extends CacheableMessage> void shutdownCallback(BlockingQueue<T> q) {
        // A callback for persistent queue implementations
        // that allows them to do some connection related cleanup when the gateway is shutting down
    }
    
    public <T extends CacheableMessage> void failureCallback(BlockingQueue<T> q, T message, int code) {
    	// A callback for persistent queue implementations
    	// to set the lastMessageId to the id of the last unsent message
        if (code != EdgeCommunicator.INVALID_REQUEST && code != EdgeCommunicator.BAD_REQUEST) {
            q.add(message);
        }
    }
}
