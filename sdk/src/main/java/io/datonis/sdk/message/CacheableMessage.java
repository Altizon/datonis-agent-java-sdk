package io.datonis.sdk.message;

import io.datonis.sdk.org.json.simple.JSONObject;

/**
 * Base class that provides ability for cacheability for objects
 * 
 * @author Rajesh Jangam (rajesh@altizon.com)
 *
 */
public abstract class CacheableMessage extends Message {
    // Message id is an internal construct used by the cached queue for persisting this data and
    // retrieving it. It has no relevance in the data that is sent across the wire.
    private long messageId;
    
    public CacheableMessage(JSONObject obj) {
        super(obj);
    }

    public CacheableMessage(String type, long timestamp, Boolean isCompressed) {
        super(type, timestamp, isCompressed);
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }
}
