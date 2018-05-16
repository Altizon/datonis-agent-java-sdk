
package io.datonis.sdk.communicator;

import io.datonis.sdk.EdgeGateway;
import io.datonis.sdk.EdgeUtil;
import io.datonis.sdk.GatewayProperties;
import io.datonis.sdk.compress.lzf.LZF;
import io.datonis.sdk.message.AlertMessage;
import io.datonis.sdk.message.BulkDataMessage;
import io.datonis.sdk.message.DataMessage;
import io.datonis.sdk.message.Instruction;
import io.datonis.sdk.message.Message;
import io.datonis.sdk.message.MessageConstants;
import io.datonis.sdk.message.MessageUtils;
import io.datonis.sdk.message.RegisterMessage;
import io.datonis.sdk.org.json.simple.JSONArray;
import io.datonis.sdk.org.json.simple.JSONAware;
import io.datonis.sdk.org.json.simple.JSONObject;
import io.datonis.sdk.org.json.simple.parser.ContainerFactory;
import io.datonis.sdk.org.json.simple.parser.JSONParser;
import io.datonis.sdk.org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Communicator that sends data using the MQTT protocol to the Datonis platform
 *
 * @author Ranjit Nair (ranjit@altizon.com)
 * @author Rajesh Jangam (rajesh@altizon.com)
 */
public class MQTTCommunicator implements EdgeCommunicator, MqttCallback {
    private static final Logger logger = LoggerFactory.getLogger(MQTTCommunicator.class);

    private static final int HTTP_ACK_MAX_RETRIES = 10;

    private static final int DATA_PACKET = 0;

    private static final int BULK_PACKET = 1;

    private static final int HEART_BEAT = 2;

    private static final int REGISTER = 3;

    private static final int ALERT = 4;

    private static final int COMPRESSED_DATA_PACKET = 5;

    private static final int COMPRESSED_BULK_PACKET = 6;

    private EdgeGateway gateway;

    private String clientId;

    private int timeout;

    private Map<Integer, Integer> qosMap;

    private Map<Integer, String> topicMap;

    private Map<String, Set<String>> thingKeys;

    private Set<String> subscribedForInstructions;

    private String broker;

    private MqttClient mqttClient;

    private ConcurrentHashMap<String, JSONObject> acks = new ConcurrentHashMap<>();

    private AtomicInteger ackCounter = new AtomicInteger();

    private JSONParser parser = new JSONParser();

    private volatile boolean connected = false;

    private volatile boolean authorised;

    public MQTTCommunicator(EdgeGateway gateway, int timeout, String brokerHost, boolean secure,
            Long port) {
        this.gateway = gateway;
        if (secure) {
            this.broker = "ssl://" + brokerHost;
            if (port == null || port == 0) {
                port = 8883L;
            }
        } else {
            this.broker = "tcp://" + brokerHost;
            if (port == null || port == 0) {
                port = 1883L;
            }
        }
        this.broker = this.broker + ":" + port.intValue();
        this.thingKeys = new ConcurrentHashMap<>();
        this.subscribedForInstructions = Collections.synchronizedSet(new HashSet<String>());
        // MQTT does not support keys more than 22 characters
        this.clientId = UUID.randomUUID().toString().substring(0, 22);
        this.timeout = timeout;
        initializeMqttParameters();
    }

    private void initializeMqttParameters() {
        topicMap = new HashMap<Integer, String>();

        qosMap = new HashMap<Integer, Integer>();

        topicMap.put(DATA_PACKET, EdgeUtil.getMqttDataTopic(clientId));
        topicMap.put(BULK_PACKET, EdgeUtil.getMqttBulkDataTopic(clientId));

        topicMap.put(COMPRESSED_DATA_PACKET, EdgeUtil.getMqttCompressedDataTopic(clientId));
        topicMap.put(COMPRESSED_BULK_PACKET, EdgeUtil.getMqttCompressedBulkDataTopic(clientId));

        qosMap.put(DATA_PACKET, 1);
        qosMap.put(BULK_PACKET, 1);

        qosMap.put(COMPRESSED_DATA_PACKET, 1);
        qosMap.put(COMPRESSED_BULK_PACKET, 1);

        topicMap.put(HEART_BEAT, EdgeUtil.getMqttHeartBeatTopic(clientId));
        qosMap.put(HEART_BEAT, 0);

        topicMap.put(REGISTER, EdgeUtil.getMqttRegisterTopic(clientId));
        qosMap.put(REGISTER, 1);

        topicMap.put(ALERT, EdgeUtil.getMqttAlertTopic(clientId));
        qosMap.put(ALERT, 1);
    }

    @Override
    public boolean isConnected() {
        return connected && mqttClient.isConnected();
    }

    @Override
    public int connect() {
        try {
            mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setConnectionTimeout(timeout);
            connOpts.setUserName(gateway.getAccessKey());
            connOpts.setPassword(MessageUtils.encode(this.gateway.getSecretKey(),this.gateway.getAccessKey()).toCharArray());
            mqttClient.connect(connOpts);
            mqttClient.setCallback(this);
            subscribeForHttpAck(clientId);
            logger.info("Connected to the MQTT Broker");
            connected = true;
            authorised = true;
        } catch (MqttSecurityException ex) {
            logger.error("Could not connect to the MQTT broker due to a permissions issue, Please check access key and secret key", ex);
            authorised = false;
            return UNAUTHORIZED;
        } catch (Exception ex) {
            logger.error("Could not connect to the MQTT broker.", ex);
            return NO_CONNECTION;
        }
        return OK;
    }

    private int postRequest(int dataType, JSONObject data, boolean compress) {
        if (data == null) {
            return INVALID_PARAMS;
        }

        if(!authorised) {
            logger.error("Unauthorised , Please check access key and secret key");
            return UNAUTHORIZED;
        }

        String topic = topicMap.get(dataType);
        int qos = qosMap.get(dataType);
        MqttMessage message = null;
        String json = data.toJSONString();
        // compress only data packets and if they are over 1 KB
        if (compress && (dataType == DATA_PACKET || dataType == BULK_PACKET)
                && (json.length() > 1024)) {
            logger.debug("Original data Size: " + json.length());
            byte[] compressedJson = null;
            compressedJson = LZF.compress(json);
            logger.debug("Compressed data Size: " + compressedJson.length);
            message = new MqttMessage(compressedJson);

            qos = qosMap.get(dataType + COMPRESSED_DATA_PACKET);
            topic = topicMap.get(dataType + COMPRESSED_DATA_PACKET);

        } else {
            message = new MqttMessage(json.getBytes());
        }

        message.setQos(qos);
        try {
            mqttClient.publish(topic, message);
        } catch (MqttSecurityException ex) {
            logger.error("There was a permissions issue while sending data: " + data, ex);
            return UNAUTHORIZED;
        } catch (MqttException ex) {
            logger.error("Could not send data: " + data, ex);
            return NO_CONNECTION;
        }
        return OK;
    }

    protected JSONObject encodeData(Message message) {
        JSONObject data = message.toJSON(true);
        String accessKey = (message.getAccessKey() == null) ? this.gateway.getAccessKey()
                : message.getAccessKey();
        String secretKey = (message.getSecretKey() == null) ? this.gateway.getSecretKey()
                : message.getSecretKey();
        data = MessageUtils.encodeObject(data, accessKey, secretKey);
        data.put(MessageConstants.PROTOCOL_VERSION, 2.0);
        return data;
    }

    private JSONObject lookupResponse(String hash) {
        JSONObject ack = acks.remove(hash);
        if (ack == null) {
            ack = acks.remove("PARSE_ERROR");
        }

        if (ackCounter.get() > 500) {
            // periodically cleanup any acks that were received late and not
            // consumed by anyone
            List<String> toRemove = new ArrayList<>();
            long currentTimestamp = System.currentTimeMillis();
            for (Entry<String, JSONObject> a : acks.entrySet()) {
                long ts = (Long)a.getValue().get("timestamp");
                if ((currentTimestamp - ts) > (5 * 60 * 1000)) {
                    toRemove.add(a.getKey());
                }
            }
            for (String k : toRemove) {
                acks.remove(k);
            }

            ackCounter.set(0);
        }
        return ack;
    }

    private int encodeAndTransmit(int dataType, Message message, boolean compress) {
        int httpCode = 408;
        JSONObject encoded = encodeData(message);
        JSONObject ackMsg = null;
        int retval = postRequest(dataType, encoded, compress);
        if (retval != OK) {
            return retval;
        }

        String hash = (String)encoded.get(MessageConstants.HASH);
        int counter = 0;
        synchronized (this) {
            // Wait for a couple of minutes before we declare failure
            while ((httpCode == 408) && (counter != HTTP_ACK_MAX_RETRIES)) {
                try {
                    wait(10 * 1000);
                } catch (InterruptedException e) {
                    // Ignore
                }

                ackMsg = lookupResponse(hash);
                if ((ackMsg != null) && ackMsg.containsKey("http_code")) {
                    httpCode = ((Number)ackMsg.get("http_code")).intValue();
                }

                if (httpCode != 408) {
                    break;
                }

                counter++;
            }
        }

        if (httpCode != 200 && (ackMsg != null) && ackMsg.containsKey("http_msg")) {
            JSONArray err_msg_array = null;
            Object httpMsg = ackMsg.get("http_msg");
            if (httpMsg instanceof JSONArray) {
                err_msg_array = (JSONArray)ackMsg.get("http_msg");
            } else if (httpMsg instanceof JSONObject) {
                err_msg_array = (JSONArray)((JSONObject)httpMsg).get("errors");
            }

            if (err_msg_array != null) {
                for (int i = 0; i < err_msg_array.size(); i++) {
                    JSONObject object = (JSONObject)err_msg_array.get(i);
                    logger.error("Error " + object.get("code") + " : " + object.get("message"));
                }
            } else if (!httpMsg.toString().isEmpty()) {
                logger.error("Error from server: " + httpMsg);
            }
        }
        return EdgeUtil.convertHTTPResponseCode(httpCode);
    }

    @Override
    public void connectionLost(Throwable exception) {
        connected = false;
        // Attempt reconnection
        int retval = INVALID_PARAMS;
        while (retval != OK) {
            logger.error(
                    "Connection to the MQTT server is lost. Attempting to connect again...");
            retval = connect();
            if (retval == OK) {
                authorised = true;
                subscribedForInstructions.clear();
                for (Entry<String, Set<String>> entry : thingKeys.entrySet()) {
                    for (String thingKey : entry.getValue()) {
                        try {
                            subscribeForThingInstructions(entry.getKey(), thingKey);
                            subscribedForInstructions.add(thingKey);
                        } catch (MqttException e) {
                            logger.error(
                                    "Could not subscribe to the MQTT server to receive instructions for thing: "
                                            + thingKey + " : " + e.getMessage(),
                                    e);
                        }
                    }
                }
            } else {
                // Sleep for 5 seconds before attempting to reconnect again
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    // Ignore this
                }
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken arg0) {
        // This can be ignored
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (topic.endsWith("executeInstruction")) {
            handleInstruction(topic, message);
        } else if (topic.endsWith("httpAck")) {
            handleHttpAck(topic, message);
        }
    }

    private void subscribeForHttpAck(String clientId) throws MqttException {
        String topic = EdgeUtil.getMqttHttpAckTopic(clientId);
        mqttClient.subscribe(topic, 1);
        logger.info("Subscribed acknowlegements");
    }

    private void subscribeForThingInstructions(String accessKey, String thingKey)
            throws MqttException {
        String topic = EdgeUtil.getMqttInstructionTopic(accessKey, thingKey);
        // Setting QOS for instructions to 2 so that they are most reliable.
        logger.info("Subscribing on instructions for thing: " + thingKey);
        mqttClient.subscribe(topic, 2);
    }

    private synchronized void handleHttpAck(String topic, MqttMessage message) {
        String payload = new String(message.getPayload());
        try {
            JSONObject ack = (JSONObject)parser.parse(payload);
            synchronized (this) {
                Integer httpCode = ((Number)ack.get("http_code")).intValue();
                String context = (String)ack.get("context");
                Object httpMsg = ack.get("http_msg");
                if ((httpMsg != null) && (httpMsg instanceof JSONAware)) {
                    JSONAware httpContent = (JSONAware)httpMsg;
                    httpMsg = httpContent.toJSONString();
                }

                if (httpMsg != null) {
                    logger.debug("Recieved http code: " + httpCode + ", context: " + context
                            + ", content: " + httpMsg);
                } else {
                    logger.debug("Recieved http code: " + httpCode + ", context: " + context);
                }
                ack.put("timestamp", System.currentTimeMillis());
                acks.put(context, ack);
                ackCounter.incrementAndGet();
                notify();
            }
        } catch (ParseException e) {
            logger.error("Could not parse HTTP Ack: " + payload + ", Error: " + e.getMessage(), e);
        }
    }

    private void handleInstruction(String topic, MqttMessage message) {
        String payload = new String(message.getPayload());
        try {
            Object obj = parser.parse(payload, new ContainerFactory() {

                @Override
                public Map createObjectContainer() {
                    return new LinkedHashMap();
                }

                @Override
                public List creatArrayContainer() {
                    return new ArrayList();
                }
            });

            LinkedHashMap parsed = (LinkedHashMap)obj;

            // First check if the hash matches
            if (!matchHashCode(parsed)) {
                // Hash code does not match. Drop the message
                logger.warn("Ignoring instruction message as Hash does not match: " + payload);
                return;
            }

            // Got a messaage to act upon.
            handleInstruction(parsed);
        } catch (ParseException ex) {
            // The message is irrelevant. Silently drop it.
        }
    }

    private boolean matchHashCode(LinkedHashMap data) {
        String hash = (String)data.remove(MessageConstants.HASH);
        data.remove(MessageConstants.ACCESS_KEY);
        return hash.equalsIgnoreCase(
                MessageUtils.encode(this.gateway.getSecretKey(), JSONObject.toJSONString(data)));
    }

    private void handleInstruction(LinkedHashMap parsed) {
        Object instruction = parsed.get(MessageConstants.INSTRUCTION_WRAPPER);
        Object thingKey = parsed.get(MessageConstants.THING_KEY);
        if (gateway.isAliasMode()) {
            thingKey = parsed.get(MessageConstants.DEVICE_KEY);
        }
        Object alertKey = parsed.get(MessageConstants.ALERT_KEY);
        Object timestamp = parsed.get(MessageConstants.TIMESTAMP);

        if (instruction == null || !(instruction instanceof LinkedHashMap)) {
            logger.warn(
                    "Instruction not passed or does not match expected format. Ignoring instruction.");
            return;
        }
        if (thingKey == null || (!(thingKey instanceof String))) {
            logger.warn(
                    "Sensor/Thing Key not passed or does not match expected format. Ignoring instruction.");
            return;
        }
        if (alertKey == null || !(alertKey instanceof String)) {
            logger.warn(
                    "Alert Key not passed or does not match expected format. Ignoring instruction.");
            return;
        }
        if (timestamp == null || (!(timestamp instanceof Long))) {
            logger.warn(
                    "Timestamp not passed or does not match expected format. Ignoring instruction. ");
            return;
        }

        Object reparsed;
        try {
            reparsed = parser.parse(JSONObject.toJSONString((LinkedHashMap)instruction));
            if (reparsed instanceof JSONObject) {
                Instruction message = new Instruction((Long)timestamp, (String)alertKey,
                        (String)thingKey, (JSONObject)reparsed);
                this.gateway.messageReceived(message);
            }
        } catch (ParseException e) {
            logger.error(
                    "Could not re-parse instruction. It will not be processed: " + e.getMessage(),
                    e);
        }
    }

    private void checkForInstructionSubscription(String thingKey, Message msg) {
        if (!subscribedForInstructions.contains(thingKey)) {
            try {
                String accessKey = (msg.getAccessKey() == null) ? this.gateway.getAccessKey()
                        : msg.getAccessKey();
                subscribeForThingInstructions(accessKey, thingKey);
                Set<String> set = thingKeys.get(accessKey);
                if (set == null) {
                    set = Collections.synchronizedSet(new HashSet<String>());
                    thingKeys.put(accessKey, set);
                }
                set.add(thingKey);
                subscribedForInstructions.add(thingKey);
            } catch (MqttException e) {
                logger.error("Could not subscribe for instructions for Thing key: " + thingKey, e);
                logger.error("Instructions will not for Thing key: " + thingKey);
            }
        }
    }

    @Override
    public int transmit(Message msg) {
        Boolean compress = (Boolean)GatewayProperties.getValue(GatewayProperties.USE_COMPRESSION);
        switch (msg.getType()) {
            case Message.DATA:
                checkForInstructionSubscription(((DataMessage)msg).getThingKey(), msg);
                return encodeAndTransmit(DATA_PACKET, msg, compress);
            case Message.BULKDATA:
                BulkDataMessage bulk = (BulkDataMessage)msg;
                for (DataMessage dm : bulk.getMessages()) {
                    checkForInstructionSubscription(dm.getThingKey(), dm);
                }
                return encodeAndTransmit(DATA_PACKET, msg, compress);
            case Message.THING_REGISTER:
                checkForInstructionSubscription(((RegisterMessage)msg).getThing().getKey(), msg);
                return encodeAndTransmit(REGISTER, msg, false);
            case Message.THING_HEARTBEAT:
                return encodeAndTransmit(HEART_BEAT, msg, false);
            case Message.ALERT:
                AlertMessage alert = (AlertMessage)msg;
                return encodeAndTransmit(ALERT, alert, false);
        }
        return EdgeCommunicator.INVALID_REQUEST;
    }

    @Override
    public void shutdown() {
        try {
            mqttClient.disconnect();
            mqttClient.close();
        } catch (MqttException e) {
            logger.error("Problem while closing MQTT connection", e);
        }
    }
}
