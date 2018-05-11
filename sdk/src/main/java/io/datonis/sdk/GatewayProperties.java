
package io.datonis.sdk;

import java.io.FileReader;

import io.datonis.sdk.org.json.simple.JSONObject;
import io.datonis.sdk.org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the class that is responsible for loading all the properties used by
 * the agent. This is a singleton so use getInstance to load up.
 *
 * @author Ranjit Nair (ranjit@altizon.com)
 */
public final class GatewayProperties {
    private static Logger logger = LoggerFactory.getLogger(GatewayProperties.class);

    // Agent properties.
    public static final String ACCESS_KEY = "access_key";
    public static final String SECRET_KEY = "secret_key";
    public static final String ALIAS_MODE = "alias_mode";
    public static final String REGISTER = "register";
    protected static final String HEARTBEAT_INTERVAL = "heartbeat_interval";
    protected static final String API_HOST = "api_host";
    protected static final String PORT = "port";
    protected static final String TIMEOUT = "request_timeout";
    protected static final String METADATA = "metadata";
    protected static final String SIMULATE = "simulate";
    protected static final String THREAD_POOL_SIZE = "thread_pool_size";
    protected static final String CONCURRENCY = "concurrency";
    protected static final String INSTRUCTION_POOL_SIZE = "instruction_pool_size";
    protected static final String BULK_TRANSMIT_INTERVAL = "bulk_transmit_interval";
    protected static final String BULK_MAX_ELEMENTS = "bulk_max_elements";
    protected static final String BULK_TRANSMIT = "bulk_transmit";
    public static final String USE_COMPRESSION = "use_compression";
    protected static final String QUEUE_SIZE = "queue_size";
    protected static final String PROTOCOL = "protocol";
    protected static final String PROXY_HOST = "proxy_host";
    protected static final String PROXY_PORT = "proxy_port";
    protected static final String PROXY_USERNAME = "proxy_username";
    protected static final String PROXY_PASSWORD = "proxy_password";
    protected static final String PROXY_DOMAIN = "proxy_domain";

    protected static final String SSH_KNOWN_HOSTS = "ssh_known_hosts_path";
    protected static final String SSH_PRIVATE_KEY = "ssh_private_key_path";
    protected static final String SSH_HOST = "ssh_host";
    protected static final String SSH_PORT = "ssh_port";
    protected static final String SSH_USERNAME = "ssh_username";
    protected static final String PRODUCT_NAME = "product_name";

    private static JSONObject properties;

    static {
        try {
            logger.info("Loading Config File ");
            loadProperties();
            logger.info("MQTT host is " + properties.get(API_HOST));
        } catch (Exception e) {
            String message = "Unable to load the agent configurations file.\nPlease ensure that the datonis-edge.properties file exists in the root or is of the correct format.\nYou can also pass a JVM parameter -Ddatonis-edge.properties to point to a valid properties file";
            logger.error(message, e);
            System.err.println(message);
            System.exit(-1);
        }
    }

    private static void loadProperties() throws Exception {
        JSONParser parser = new JSONParser();
        String propertiesFilePath =  System.getProperty("datonis-edge.properties");
        if (propertiesFilePath == null) {
            propertiesFilePath =  System.getProperty("aliot.properties");
            if (propertiesFilePath == null) {
                propertiesFilePath = "datonis-edge.properties";
            }
        }
        Object obj = parser.parse(new FileReader(propertiesFilePath));
        properties = (JSONObject)obj;

        if (!properties.containsKey(HEARTBEAT_INTERVAL))
            properties.put(HEARTBEAT_INTERVAL, (long)120000);

        if (!properties.containsKey(TIMEOUT))
            properties.put(TIMEOUT, (long)10000);

        if (!properties.containsKey(SIMULATE))
            properties.put(SIMULATE, Boolean.FALSE);

        if (!properties.containsKey(THREAD_POOL_SIZE))
            properties.put(THREAD_POOL_SIZE, (long)5);

        // If someone used the new concurrency configuration
        if (properties.containsKey(CONCURRENCY)) {
            properties.put(THREAD_POOL_SIZE, properties.get(CONCURRENCY));
        }

        if (!properties.containsKey(BULK_TRANSMIT_INTERVAL))
            properties.put(BULK_TRANSMIT_INTERVAL, (long)60000);

        if (!properties.containsKey(BULK_MAX_ELEMENTS))
            properties.put(BULK_MAX_ELEMENTS, (long)25);

        if (!properties.containsKey(BULK_TRANSMIT))
            properties.put(BULK_TRANSMIT, Boolean.FALSE);

        if (!properties.containsKey(USE_COMPRESSION))
            properties.put(USE_COMPRESSION, Boolean.FALSE);

        if (!properties.containsKey(QUEUE_SIZE))
            properties.put(QUEUE_SIZE, (long)100);
        if (!properties.containsKey(PROTOCOL))
            properties.put(PROTOCOL, "https");

        if (!properties.containsKey(API_HOST)) {
            if (properties.get(PROTOCOL).equals("mqtt") || properties.get(PROTOCOL).equals("mqtts")) {
                logger.info("MQTT PROTOCOL connecting to mqttbroker");
                properties.put(API_HOST, "mqtt-broker.datonis.io");
            }
            else
                properties.put(API_HOST, "api.datonis.io");
        }

        if (!properties.containsKey(INSTRUCTION_POOL_SIZE)) {
            properties.put(INSTRUCTION_POOL_SIZE, (long)5);
        }
    }

    public static Object getValue(String key) {
        Object value = properties.get(key);
        if (value != null && value instanceof String) {
            // Learnt this the hard way. Sometimes property values have
            // whitespaces at the end which can cause unnecessary anguish
            value = ((String)value).trim();
        }
        return value;
    }
}
