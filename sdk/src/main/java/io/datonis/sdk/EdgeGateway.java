
package io.datonis.sdk;

import io.datonis.sdk.communicator.EdgeCommunicator;
import io.datonis.sdk.communicator.MQTTCommunicator;
import io.datonis.sdk.communicator.RESTCommunicator;
import io.datonis.sdk.communicator.SimulateCommunicator;
import io.datonis.sdk.exception.EdgeGatewayException;
import io.datonis.sdk.exception.IllegalThingException;
import io.datonis.sdk.message.AlertMessage;
import io.datonis.sdk.message.AlertType;
import io.datonis.sdk.message.BulkDataMessage;
import io.datonis.sdk.message.BulkHeartbeatMessage;
import io.datonis.sdk.message.DataMessage;
import io.datonis.sdk.message.HeartbeatMessage;
import io.datonis.sdk.message.Instruction;
import io.datonis.sdk.message.Message;
import io.datonis.sdk.message.RegisterMessage;
import io.datonis.sdk.org.json.simple.JSONArray;
import io.datonis.sdk.org.json.simple.JSONObject;
import io.datonis.sdk.org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

/**
 * Gateway Interface to send data to the Datonis Platform
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 * @author Rajesh Jangam (rajesh@altizon.com)
 */
public class EdgeGateway {
    private static final Logger logger = LoggerFactory.getLogger(EdgeGateway.class);

    private String accessKey;
    private String secretKey;
    private BlockingQueue<DataMessage> dataQueue;
    private BlockingQueue<Message> handshakeQueue;
    private BlockingQueue<AlertMessage> alertQueue;
    private BlockingQueue<Instruction> instructionQueue;
    private volatile InstructionHandler instructionHandler;
    private QueueFactory queueFactory;
    private Map<String, Thing> things = new ConcurrentHashMap<>();
    private List<Thread> threads = Collections.synchronizedList(new ArrayList<Thread>());
    private ExecutorService instructionPool;
    private EdgeCommunicator communicator;
    private volatile boolean shutdown;
    private volatile boolean registered;
    private volatile boolean connected;
    private boolean isBidirectionalGateway;
    private Boolean isAliasMode;
    private Long timeout = 180* 1000L;
    private String apiHost;
    private String protocol;
    private Long port;
    private String proxyHost;
    private Long proxyPort;
    private String proxyUsername;
    private String proxyPassword;
    private String proxyDomain;
    private String productName = "Datonis";

    public EdgeGateway() {
        this(new QueueFactory());
    }

    public EdgeGateway(QueueFactory queueFactory) {
        this.queueFactory = queueFactory;
    }

    public String getAccessKey() {
        return this.accessKey;
    }

    public String getSecretKey() {
        return this.secretKey;
    }

    public Boolean isAliasMode() {
        return this.isAliasMode;
    }

    private EdgeCommunicator createCommunicator() {
        timeout = (Long) GatewayProperties.getValue(GatewayProperties.TIMEOUT);
        if (timeout == null) {
            timeout = 180 * 1000L;
        }

        Boolean simulate = (Boolean) GatewayProperties.getValue(GatewayProperties.SIMULATE);
        // In the simulate mode, we simply 'trust' that the message can be transmitted.
        if (simulate.equals(Boolean.TRUE)) {
            return new SimulateCommunicator();
        }
        apiHost = (String) GatewayProperties.getValue(GatewayProperties.API_HOST);
        protocol = (String) GatewayProperties.getValue(GatewayProperties.PROTOCOL);
        port = (Long) GatewayProperties.getValue(GatewayProperties.PORT);
        
        proxyHost = (String) GatewayProperties.getValue(GatewayProperties.PROXY_HOST);
        proxyPort = (Long) GatewayProperties.getValue(GatewayProperties.PROXY_PORT);
        proxyUsername = (String) GatewayProperties.getValue(GatewayProperties.PROXY_USERNAME);
        proxyPassword = (String) GatewayProperties.getValue(GatewayProperties.PROXY_PASSWORD);
        proxyDomain = (String) GatewayProperties.getValue(GatewayProperties.PROXY_DOMAIN);
        if (protocol == null || "HTTP".equalsIgnoreCase(protocol)) {
            return new RESTCommunicator(this, timeout.intValue(), apiHost, false, port, proxyHost, proxyPort, proxyUsername, proxyPassword, proxyDomain);
        } else if ("HTTPS".equalsIgnoreCase(protocol)) {
            return new RESTCommunicator(this, timeout.intValue(), apiHost, true, port, proxyHost, proxyPort, proxyUsername, proxyPassword, proxyDomain);
        } else if ("MQTT".equalsIgnoreCase(protocol)) {
            isBidirectionalGateway = true;
            return new MQTTCommunicator(this, timeout.intValue(), apiHost, false, port);
        } else if ("MQTTS".equalsIgnoreCase(protocol)) {
            isBidirectionalGateway = true;
            return new MQTTCommunicator(this, timeout.intValue(), apiHost, true, port);
        } else {
            logger.error("Unsupported protocol: " + protocol + ", messages will not be sent");
            return new SimulateCommunicator();
        }
    }

    /**
     * Sets the event id for the event that is being published. By default, this is installid_agentid
     * 
     */
    private void initializeGateway() {
        accessKey = (String) GatewayProperties.getValue(GatewayProperties.ACCESS_KEY);
        secretKey = (String) GatewayProperties.getValue(GatewayProperties.SECRET_KEY);
        Boolean register = (Boolean) GatewayProperties.getValue(GatewayProperties.REGISTER);
        if (accessKey == null || secretKey == null) {
            // Pretty much an impossible situation. Yell and bail.
            logger.error("Gateway is installed without an access key, secret key");
            stop();
        }

        isAliasMode = (Boolean) GatewayProperties.getValue(GatewayProperties.ALIAS_MODE);
        if (isAliasMode == null) {
            isAliasMode = false;
        }

        long queueSize = (Long) GatewayProperties.getValue(GatewayProperties.QUEUE_SIZE);
        // Initialize the queue to which the agent will send messages.
        dataQueue = queueFactory.createQueue((int) queueSize, "EdgeDataDB", DataMessage.class);
        alertQueue = queueFactory.createQueue((int) queueSize, "EdgeAlertDB", AlertMessage.class);
        handshakeQueue = new LinkedBlockingQueue<Message>(500);
        communicator = createCommunicator();
        if (communicator.connect() != EdgeCommunicator.OK) {
            final int connectionTimeout = 60;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    do {
                        logger.warn("Cannot connect to the " + productName + " server. Will try again after " + connectionTimeout + " seconds");
                        try {
                            Thread.sleep(connectionTimeout * 1000);
                        } catch (InterruptedException e) {
                        }
                    } while (!isShutdown() && (communicator.connect() != EdgeCommunicator.OK));
                }
            }, "Connection-Thread");
            t.start();
            threads.add(t);
        }
        
        // No register unless explicitly asked for
        if (register == null || register == false) {
            registered = true;
        }

        if (isBidirectionalGateway) {
            Long instructionPoolSize = (Long) GatewayProperties.getValue(GatewayProperties.INSTRUCTION_POOL_SIZE);
            instructionPool = Executors.newFixedThreadPool(instructionPoolSize.intValue(), new ThreadFactory() {
                private AtomicInteger i = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "Instruction-Handler-" + i.incrementAndGet());
                }
            });
            instructionQueue = new LinkedBlockingQueue<>();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    boolean done = false;
                    while (!done) {
                        try {
                            final Instruction instruction = instructionQueue.take();
                            if (instruction != null && instructionHandler != null) {
                                instructionPool.submit(new Runnable() {

                                    @Override
                                    public void run() {
                                        try {
                                            instructionHandler.handleInstructionExecution(EdgeGateway.this, instruction);
                                        } catch (Exception e) {
                                            logger.error("Unhandled exception while executing instruction", e);
                                        }
                                    }
                                });
                            }
                        } catch (InterruptedException e) {
                            if (EdgeGateway.this.isShutdown()) {
                                done = true;
                                logger.info("Instruction handling main thread shutting down");
                            }
                        }
                    }
                }
            }, "Main-Instruction-Handler");
            t.start();
            threads.add(t);
        }
    }

    private void initializeMessageConsumer() {
        Boolean bulk = (Boolean) GatewayProperties.getValue(GatewayProperties.BULK_TRANSMIT);
        GatewayMessageConsumer messageConsumer = null;
        if (bulk.equals(Boolean.TRUE)) {
            final long maxElements = (Long) GatewayProperties.getValue(GatewayProperties.BULK_MAX_ELEMENTS);
            final Long bulkInterval = (Long) GatewayProperties.getValue(GatewayProperties.BULK_TRANSMIT_INTERVAL);
            messageConsumer = new GatewayMessageConsumer((Long)GatewayProperties.getValue(GatewayProperties.THREAD_POOL_SIZE), this, dataQueue, communicator, bulkInterval) {
                @Override
                protected Message getNextMessage(BlockingQueue<? extends Message> queue) throws InterruptedException {
                    List messages = new ArrayList<>();
                    while (messages.size() == 0) {
                        queue.drainTo(messages, (int) maxElements);
                        logger.info("Got a drain of " + messages.size() + ", queue size: " + queue.size());
                        if (messages.size() == 0) {
                            // Wait for sometime before data becomes available
                            Thread.sleep(1000);
                        }
                    }
                    // Send the data on its way.
                    BulkDataMessage bulkMessage = new BulkDataMessage(messages, true);
                    return bulkMessage;
                }
            };
        } else {
            messageConsumer = new GatewayMessageConsumer((Long) GatewayProperties.getValue(GatewayProperties.THREAD_POOL_SIZE), this, dataQueue, communicator, null);
        }
        // Register for transmission events
        Thread t = new Thread(messageConsumer, "Data-Consumer");
        t.start();
        threads.add(t);
    }

    private void initializeConsumer(String name, GatewayMessageConsumer consumer) {
        Thread t = new Thread(consumer, name);
        t.start();
        threads.add(t);
    }

    private void initializeMonitor() {
        Thread t = new Thread(new GatewayMonitor(this), "Gateway-Monitor");
        t.start();
        threads.add(t);
    }

    public void start() {
        productName = (String)GatewayProperties.getValue(GatewayProperties.PRODUCT_NAME);
        if (productName == null) {
            productName = "Datonis";
        }

        logger.info("Starting up the " + productName + " Edge Gateway");
        Boolean simulate = (Boolean)GatewayProperties.getValue(GatewayProperties.SIMULATE);
        // In the simulate mode, we simply 'trust' that the message can be
        // transmitted.
        if (simulate.equals(Boolean.TRUE)) {
            logger.info("Running in simulate mode");
        } else {
            logger.info("Agent will communicate using " + GatewayProperties.getValue(GatewayProperties.PROTOCOL));
        }
        initializeGateway();
        initializeConsumer("Handshake-Consumer", new GatewayMessageConsumer(1L, this, handshakeQueue, communicator, true, null));
        initializeMessageConsumer();
        initializeConsumer("Alert-Consumer", new GatewayMessageConsumer((Long) GatewayProperties.getValue(GatewayProperties.THREAD_POOL_SIZE), this, alertQueue, communicator, null));
        initializeMonitor();
        while (!registered) {
            logger.info("Waiting to register Things");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        logger.info("Successfully started the " + productName + " Edge Gateway");
    }

    public boolean addThing(Thing thing) {
        if (thing == null)
            return false;

        // Cannot add two things with the same key.
        if (things.containsKey(thing.getKey()))
            return false;
        things.put(thing.getKey(), thing);
        return true;
    }

    private boolean transmitHandshake(Message message) {
        try {
            if ((handshakeQueue.remainingCapacity() > 5) || !message.getType().equalsIgnoreCase(Message.THING_HEARTBEAT)) {  
                handshakeQueue.put(message);
            }
            return true;
        } catch (InterruptedException e) {
            logger.error("Error sending a handshake message ", e);
        }
        return false;
    }

    private boolean transmitThingHeartbeat(long timestamp) {
        boolean retVal = false;
        for (Thing thing : things.values()) {
            HeartbeatMessage message = new HeartbeatMessage(thing, isAliasMode, timestamp);
            retVal = transmitHandshake(message);
        }
        return retVal;
    }
    
    private boolean transmitBulkThingHeartbeat(long timestamp) {
        boolean retVal = false;
        ArrayList<Thing> thingList = new ArrayList<Thing>();
        for (Thing thing : things.values()) {
            thingList.add(thing);
        }
        BulkHeartbeatMessage message =  new BulkHeartbeatMessage(thingList, timestamp);
        retVal = transmitHandshake(message);
        return retVal;
    }

    public boolean transmitBulkHeartbeat() {
        long timestamp = System.currentTimeMillis();
        return transmitBulkThingHeartbeat(timestamp);
    }

    public boolean transmitHeartbeat() {
        long timestamp = System.currentTimeMillis();
        return transmitThingHeartbeat(timestamp);
    }

    private <T> boolean transmit(Thing thing, T message, BlockingQueue<T> queue) throws IllegalThingException {
        if (thing == null)
            return false;

        try {
            queue.put(message);
            return true;
        } catch (InterruptedException e) {
            logger.error("Error sending a message" + e.getMessage(), e);
        }
        return false;
    }

    /**
     * Transmit the data over the wire. The data is in the form of a JSON object
     * with key value pairs. Timestamp corresponds to the exact time that the
     * data needs to be sent by.
     * 
     * @return true if the data is successfully transmitted.
     * @throws IllegalThingException
     */
    public boolean transmitData(Thing thing, JSONObject data, JSONArray waypoint, long timestamp, Boolean isCompressed) throws IllegalThingException {
        DataMessage d = new DataMessage((thing == null ? "" : thing.getKey()), isAliasMode, timestamp, data, waypoint, isCompressed);
        d.setAccessKey(thing.getAccessKey());
        d.setSecretKey(thing.getSecretKey());
        return transmit(thing, d, dataQueue);
    }
    
    public int transmitDataSynchronously(Thing thing, JSONObject data, JSONArray waypoint, long timestamp, Boolean isCompressed) {
        DataMessage d = new DataMessage((thing == null ? "" : thing.getKey()), isAliasMode, timestamp, data, waypoint, isCompressed);
        d.setAccessKey(thing.getAccessKey());
        d.setSecretKey(thing.getSecretKey());
        return communicator.transmit(d);
    }

    /**
     * Transmit the data over the wire. The data is in the form of a JSON object
     * with key value pairs.
     * 
     * @return true if the data is successfully transmitted.
     * @throws IllegalThingException if the thing is invalid
     */
    public boolean transmitData(Thing thing, JSONObject data, JSONArray waypoint) throws IllegalThingException {
        return transmitData(thing, data, waypoint, System.currentTimeMillis(), false);
    }

    public boolean transmitCompressedData(Thing thing, JSONObject data, JSONArray waypoint) throws IllegalThingException {
        return transmitData(thing, data, waypoint, System.currentTimeMillis(), true);
    }

    public int transmitAlertSynchronously(String alertKey, String thingKey, AlertType alertType, String message, JSONObject data, long timestamp) {
       return communicator.transmit(new AlertMessage(alertKey, thingKey, isAliasMode, alertType, message, data));
    }

    public boolean transmitAlert(String alertKey, String thingKey, AlertType alertType, String message, JSONObject data, long timestamp) {
        try {
            Thing thing = new Thing(thingKey, "", "");
            return transmit(thing, new AlertMessage(alertKey, thingKey, isAliasMode, alertType, message, data), alertQueue);
        } catch (IllegalThingException e) {
            logger.error("Could not transmit alert", e);
            return false;
        }
    }

    public boolean transmitAlert(String alertKey, String thingKey, AlertType alertType, String message, JSONObject data) {
        return transmitAlert(alertKey, thingKey, alertType, message, data, System.currentTimeMillis());
    }

    public boolean transmitAlert(String thingKey, AlertType alertType, String message, JSONObject data) {
        return transmitAlert(null, thingKey, alertType, message, data, System.currentTimeMillis());
    }

    public boolean transmitAlert(String thingKey, AlertType alertType, String message) throws IllegalThingException {
        return transmitAlert(thingKey, alertType, message, null);
    }

    /**
     * Indicates if the agent is connected or not
     * 
     * @return true if it is connected.
     */
    protected boolean isConnected() {
        return connected;
    }

    public boolean isRegistered() {
        return registered;
    }

    public boolean isShutdown() {
        return shutdown;
    }    

    protected boolean register() {
        boolean retVal = false;
        // Iterate through the thing list and send a registration request
        // for each of them.
        for (Thing thing : things.values()) {
            if (!this.isBidirectionalGateway) {
                thing.setBiDirectional(false);
            } else {
                thing.setBiDirectional(true);
            }
            RegisterMessage message = new RegisterMessage(thing, isAliasMode);
            retVal = transmitHandshake(message);
        }
        return retVal;
    }

    private void waitFor(Thread thread) {
        if (thread == null)
            return;
        try {
            thread.interrupt();
            thread.join();
        } catch (InterruptedException e) {
            logger.error("Could not wait for thread: " + thread.getName(), e);
        }
    }

    /**
     * Gracefully shuts down the agent
     */
    public synchronized void stop() {
        // First shut down the heartbeat and retry threads.
        shutdown = true;
        for (Thread t : threads) {
            waitFor(t);
        }
        queueFactory.shutdownCallback(dataQueue);
        queueFactory.shutdownCallback(alertQueue);
        communicator.shutdown();
        logger.info(productName + " Edge Gateway has shut down");
    }

    public synchronized void setInstructionHandler(InstructionHandler handler) {
        if (this.isBidirectionalGateway) {
            logger.info("Setting up instruction handler for bi-directional communication");
            this.instructionHandler = handler;
        }
    }

    public void messageTransmitted(Message message) {
        connected = true;
        String messageType = message.getType();
        switch (messageType) {
	        case Message.THING_REGISTER:
				registered = true;
				break;
	        case Message.DATA:
                // If a message is successfully transmitted, remove it from the cache as
                // it has successfully exited the system
	            logger.debug("Invoking cleanup callback on queuefactory");
                queueFactory.cleanUpCallback(dataQueue, Arrays.asList((DataMessage)message));
                break;
            case Message.BULKDATA:
                Collection<DataMessage> messages = ((BulkDataMessage)message).getMessages();
                queueFactory.cleanUpCallback(dataQueue, messages);
                break;
            case Message.ALERT:
                queueFactory.cleanUpCallback(alertQueue, Arrays.asList((AlertMessage)message));
                break;
        }
    }

    public void messageFailed(Message message, int code) {
    	try {
	        connected = false;
	        String messageType = message.getType();
	        switch (messageType) {
	            case Message.THING_REGISTER:
	                registered = false;
	                break;
	            case Message.DATA:
	            	queueFactory.failureCallback(dataQueue, (DataMessage)message, code);
	                break;
	            case Message.BULKDATA:
	            	Message msg = null;
	                Collection<DataMessage> messages = ((BulkDataMessage)message).getMessages();
	                Iterator<DataMessage> iter = messages.iterator();
	                  
	                while(iter.hasNext()){
	                	msg = iter.next();
	                	queueFactory.failureCallback(dataQueue, (DataMessage)msg, code);
	                	logger.debug("Message is: " + msg.toString());
	                }
	                
	                break;
	            case Message.ALERT:
	            	queueFactory.failureCallback(alertQueue, (AlertMessage)message, code);
	                break;
	        }
	
	        // First check if the message is rejected as it is unauthorized. If so,
	        // shut down the agent.
	        if (message.getTransmitStatus() == EdgeCommunicator.UNAUTHORIZED) {
	            registered = false;
	            stop();
	            return;
	
	        }
    	} catch(Exception e) {
    		logger.error("Failed Message handling was unsuccessful",e);
    	}
    }

    public void messageReceived(Message message) {
        String messageType = message.getType();
        switch (messageType) {
            case Message.INSTRUCTION:
                instructionQueue.add((Instruction)message);
                break;
            default:
                logger.warn("Message cannot be handled, ignoring it: " + message.toJSON());
        }
    }

    private String getConfiguration(String name) throws EdgeGatewayException {
        String s = (String) GatewayProperties.getValue(name);
        if (s == null || s.trim().length() == 0) {
            throw new EdgeGatewayException("Input " + name + " needs to be specified in the gateway properties file");
        }
        return s;
    }

    public void downloadFileUsingSftp(String sourcePath, String destPath, boolean downloadFromParent) throws EdgeGatewayException {
        String host = "unknown";
        Session session = null;
        Channel channel = null;
        try {
            JSch jsch = new JSch();
            String username = getConfiguration(GatewayProperties.SSH_USERNAME);
            String knownHostsPath = getConfiguration(GatewayProperties.SSH_KNOWN_HOSTS);
            String privateKeyPath = getConfiguration(GatewayProperties.SSH_PRIVATE_KEY);
            host = getConfiguration(GatewayProperties.SSH_HOST);
            Long port = (Long)GatewayProperties.getValue(GatewayProperties.SSH_PORT);
            jsch.addIdentity(privateKeyPath);
            if (port != null) {
                session = jsch.getSession(username, host, port.intValue());
            } else {
                session = jsch.getSession(username, host);
            }
            jsch.setKnownHosts(knownHostsPath);

            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftp = (ChannelSftp)channel;
            String newSourcePath = (downloadFromParent ? "PARENT_FILE_STORAGE" : "FILE_STORAGE") + sourcePath;
            sftp.get(newSourcePath, destPath, new SftpProgressMonitor() {
                private double max;
                private double soFar = 0;

                @Override
                public void init(int op, String src, String dest, long max) {
                    logger.info("Starting download using SFTP " + src + " -> " + dest + " total: " + max);
                    this.max = max;
                }

                @Override
                public void end() {
                    logger.info("Finished download");
                }

                @Override
                public boolean count(long bytes) {
                    long prevPercent = Math.round(100.0 * soFar / max);
                    soFar += bytes;
                    long newPercent = Math.round(100.0 * soFar / max);

                    if (newPercent != prevPercent) {
                        logger.info("Download progress: " + newPercent + " %");
                    }

                    return(true);
                }
            });
        } catch (JSchException e) {
            logger.error("Could not initiate SFTP session with host: " + host, e);
            throw new EdgeGatewayException("Could not initialize SFTP session with host: " + host, e);
        } catch (SftpException e) {
            logger.error("Could not download file " + sourcePath + " to destination: " + destPath, e);
            throw new EdgeGatewayException("Could not download file " + sourcePath + " to destination: " + destPath, e);
        } finally {
            try {
                if (channel != null) {
                    channel.disconnect();
                }
                if (session != null) {
                    session.disconnect();
                }
            } catch (Exception e) {
                logger.warn("Error while disconnecting SFTP client.", e);
            }
        }
    }

    public void downloadFileUsingSftp(String sourcePath, String destPath) throws EdgeGatewayException {
        downloadFileUsingSftp(sourcePath, destPath, false);
    }

    public void downloadFileUsingHttp(String sourcePath, String destPath) throws EdgeGatewayException {
        File f = new File(sourcePath);
        try {
            logger.info("Starting download using http " + sourcePath + " -> " + destPath);
            HttpClientBuilder clientBuilder = HttpClientBuilder.create();
            clientBuilder.useSystemProperties();
            Long timeout = 300 * 1000L;
            RequestConfig config = RequestConfig.copy(RequestConfig.DEFAULT).setSocketTimeout(timeout.intValue()).setConnectTimeout(timeout.intValue())/*.setConnectionRequestTimeout(timeout)*/.build();
            clientBuilder.setDefaultRequestConfig(config);
            if ((proxyHost != null) && (proxyPort != null)) {
                if (proxyUsername != null & proxyPassword != null) {
                    CredentialsProvider credsProvider = new BasicCredentialsProvider();
                    Credentials creds = null;
                    if (proxyDomain != null) {
                        String hostname = null;
                        try {
                            hostname = InetAddress.getLocalHost().getHostName();
                        } catch (Exception e) {
                            logger.warn("Could not get hostname", e);
                        }
                        creds = new NTCredentials(proxyUsername, proxyPassword, hostname, proxyDomain);
                    } else {
                        creds = new UsernamePasswordCredentials(proxyUsername, proxyPassword);
                    }
                    credsProvider.setCredentials(new AuthScope(proxyHost, proxyPort.intValue()), creds);
                    clientBuilder.setDefaultCredentialsProvider(credsProvider);
                }
                clientBuilder.setProxy(new HttpHost(proxyHost, proxyPort.intValue()));
                clientBuilder.setProxyAuthenticationStrategy(new ProxyAuthenticationStrategy());
            }
            CloseableHttpClient httpClient = clientBuilder.build();
            URIBuilder builder = new URIBuilder();

            builder.setScheme("https").setHost(apiHost).setPath(EdgeUtil.getDownloadFileUrl("")).setParameter("path", f.getParent())
                .setParameter("file_name", f.getName());
            URI uri = builder.build();
            HttpGet request = new HttpGet(uri);
            request.setHeader("X-Access-Key", accessKey);
            CloseableHttpResponse response = httpClient.execute(request);
            int code = response.getStatusLine().getStatusCode();
            if (code != 200) {
                JSONParser parser = new JSONParser();
                JSONObject j = (JSONObject) parser.parse(new InputStreamReader(response.getEntity().getContent()));
                JSONArray a = (JSONArray)j.get("errors");
                JSONObject error = (JSONObject)a.get(0);
                String datonisCode = (String)error.get("code");
                String datonisMessage = (String)error.get("message");
                logger.error("Could not download file: " + sourcePath + ", response code: " + code + ", server code: " + datonisCode + ", server message: " + datonisMessage);
                throw new EdgeGatewayException("Could not download file: " + sourcePath + ", response code: " + code + ", server code: " + datonisCode + ", server message: " + datonisMessage);
            } else {
                FileOutputStream o = new FileOutputStream(destPath + "/" + f.getName());
                try {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        entity.writeTo(o);
                        logger.info("Finished download");
                    }
                } finally {
                    o.close();
                }
            }
        } catch (Exception e) {
            logger.error("Could not download file: " + sourcePath, e);
            throw new EdgeGatewayException("Could not download file " + sourcePath, e);
        }
    }
}
