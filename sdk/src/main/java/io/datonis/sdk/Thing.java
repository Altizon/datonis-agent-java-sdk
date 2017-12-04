
package io.datonis.sdk;

import io.datonis.sdk.exception.IllegalThingException;

import io.datonis.sdk.org.json.simple.JSONObject;
import io.datonis.sdk.org.json.simple.parser.JSONParser;
import io.datonis.sdk.org.json.simple.parser.ParseException;

/**
 * Object that represents a Data Stream
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 * @author Rajesh Jangam (rajesh@altizon.com)
 */
public class Thing {
    private String thingKey;
    private String thingName;
    private String thingDescription;
    private boolean isBiDirectional;
    private String accessKey;
    private String secretKey;

    public Thing(String thingKey, String thingName, String thingDescription) throws IllegalThingException {
        if (thingKey == null || thingName == null) {
            throw new IllegalThingException(this, "A Thing (formerly sensor) requires a key, type, name and metadata");
        }

        this.thingKey = thingKey;
        this.thingDescription = thingDescription;
        this.thingName = thingName;
    }

    public Thing(String thingKey, String thingName, String thingDescription,
            boolean biDirectional) throws IllegalThingException {
        this(thingKey, thingName, thingDescription);
        this.isBiDirectional = biDirectional;
    }
    
    public Thing(String thingKey, String thingName, String thingDescription,
            boolean biDirectional, String accessKey, String secretKey) throws IllegalThingException {
        this(thingKey, thingName, thingDescription);
        this.isBiDirectional = biDirectional;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getKey() {
        return thingKey;
    }

    public String getAccessKey() {
        return this.accessKey;
    }
    
    public String getSecretKey() {
        return this.secretKey;
    }

    public String getName() {
        return thingName;
    }

    public String getDescription() {
        return thingDescription;
    }

    public boolean isBiDirectional() {
        return this.isBiDirectional;
    }

    public void setBiDirectional(boolean direction) {
        this.isBiDirectional = direction;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Thing))
            return false;

        Thing thing = (Thing)obj;

        if (thing.getKey().equalsIgnoreCase(thingKey))
            return true;

        if (thing.getName().equalsIgnoreCase(thingName))
            return true;

        return false;
    }

}
