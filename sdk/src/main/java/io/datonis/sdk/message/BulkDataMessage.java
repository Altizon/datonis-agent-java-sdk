package io.datonis.sdk.message;

import java.util.Collection;

import io.datonis.sdk.org.json.simple.JSONArray;
import io.datonis.sdk.org.json.simple.JSONObject;

/**
 * Represents a bunch of data packets packed into a bulk message
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 */
public class BulkDataMessage extends Message
{
    private Collection<DataMessage> messages;

    public BulkDataMessage(Collection<DataMessage> messages, Boolean isCompressed)
    {
        super(Message.BULKDATA, System.currentTimeMillis(), isCompressed);
        this.messages = messages;
    }

    public Collection<DataMessage> getMessages()
    {
        return messages;
    }
    
    @Override
    public JSONObject toJSON() {
        JSONObject json = super.toJSON();

        if (messages == null || messages.isEmpty())
            return json;

        JSONArray messageList = new JSONArray();
        for (DataMessage message : messages) {
            messageList.add(message.toJSON());
        }
        json.put(MessageConstants.EVENTS, messageList);
        return json;
    }
    
    @Override
    public JSONObject toJSON(boolean skipUnwanted) {
        JSONObject json = this.toJSON();
        if (skipUnwanted) {
            json.remove(MessageConstants.TYPE);
            json.remove(MessageConstants.COMPRESSED);
            JSONArray messages = (JSONArray)json.get(MessageConstants.EVENTS);
            for (int i = 0; i < messages.size(); i++) {
                JSONObject event = (JSONObject)messages.get(i);
                event.remove(MessageConstants.TYPE);
                event.remove(MessageConstants.COMPRESSED);
            }
        }
        return json;
    }
}
