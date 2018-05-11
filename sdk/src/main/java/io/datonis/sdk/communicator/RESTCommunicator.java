package io.datonis.sdk.communicator;

import io.datonis.sdk.EdgeGateway;
import io.datonis.sdk.EdgeUtil;
import io.datonis.sdk.message.AlertMessage;
import io.datonis.sdk.message.Message;
import io.datonis.sdk.message.MessageUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import io.datonis.sdk.org.json.simple.JSONArray;
import io.datonis.sdk.org.json.simple.JSONObject;
import io.datonis.sdk.org.json.simple.parser.JSONParser;
import io.datonis.sdk.org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP based communicator that pushes data to Datonis
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 * @author Rajesh Jangam (rajesh@altizon.com)
 */
public class RESTCommunicator implements EdgeCommunicator {
    private static final Logger logger = LoggerFactory.getLogger(RESTCommunicator.class);
    private String apiHost;
    private Long apiPort;
    private String protocol;
    private String dataURL;
    private String thingHeartBeatURL;
    private String thingRegisterURL;
    private String createAlertURL;
    private EdgeGateway gateway;
    private CloseableHttpClient httpClient;

    public RESTCommunicator(EdgeGateway gateway, int timeout, String apiHost, boolean secure, Long port, final String proxyHost, final Long proxyPort, final String proxyUsername, final String proxyPassword, final String proxyDomain) {
        this.gateway = gateway;
        this.apiHost = apiHost;
        this.apiPort = port;
        

        this.protocol = "https";
        this.apiPort = 443L;
        if (!secure) {
            protocol = "http";
            this.apiPort = 80L;
        }
        
        if (port != null && port != 0L) {
            this.apiPort = port;
        }
        
        this.dataURL = EdgeUtil.getDataUrl("");
        this.thingHeartBeatURL = EdgeUtil.getHeartBeatUrl("");
        this.thingRegisterURL = EdgeUtil.getRegisterUrl("");
        this.createAlertURL = EdgeUtil.getCreateAlertUrl("");
        
        
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.useSystemProperties();
        RequestConfig config = RequestConfig.copy(RequestConfig.DEFAULT).setSocketTimeout(timeout).setConnectTimeout(timeout)/*.setConnectionRequestTimeout(timeout)*/.build();
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
        this.httpClient = clientBuilder.build();
    }
    
    @Override
    public boolean isConnected() {
        return true;
    }


    private int sendRequest(String method, String urlString, String json, String accessKey,
            String hash, boolean compress) {
        if (urlString == null || json == null)
            return INVALID_PARAMS;

        if (accessKey == null || hash == null)
            return INVALID_PARAMS;
        
        HttpHost target = new HttpHost(apiHost, apiPort.intValue(), protocol);

        int retval = -1;
        String error = null;
        int responseCode = 401;
        CloseableHttpResponse response = null;
        try {
            HttpEntityEnclosingRequestBase request = null;
            if (method.equalsIgnoreCase("POST")) {
                request = new HttpPost(urlString);
            } else {
                request = new HttpPut(urlString);
            }
            request.setHeader("X-Access-Key", accessKey);
            request.setHeader("X-Dtn-Signature", hash);
            request.setEntity(new StringEntity(json));
            logger.debug("Before execute in REST communicator");
            response = httpClient.execute(target, request);
            logger.debug("After execute in REST communicator");
            responseCode = response.getStatusLine().getStatusCode();
            retval = EdgeUtil.convertHTTPResponseCode(responseCode);
            if (retval != EdgeCommunicator.OK) {
                HttpEntity entity = response.getEntity();
                InputStream responsemsg = entity.getContent();
                BufferedReader br = new BufferedReader(new InputStreamReader(responsemsg));
                String line = null;
                error = "";
                while ((line = br.readLine()) != null) {
                    error += line + "\n";
                }
                br.close();
                error = error.trim();
            }
        } catch (Exception e) {
            retval = FAILED;
            logger.error("Unable to post a request : " + e.getMessage(), e);
            retval = EdgeUtil.convertHTTPResponseCode(responseCode);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
            
            }
        }
        
        if (retval != EdgeCommunicator.OK && (error != null) && (!error.isEmpty())) {
        	JSONParser parser = new JSONParser();
        	JSONArray err_msg_array = null;
        	try {
        		Object obj = parser.parse(error);
        		if (obj instanceof JSONArray) {
        			err_msg_array = (JSONArray) obj;
        		} else if (obj instanceof JSONObject){
        			JSONObject jsonObject = (JSONObject) obj;
                	err_msg_array = (JSONArray) jsonObject.get("errors");
        		} else {
                    logger.error("Error from server: " + error);
        		}
        		
        		if (err_msg_array != null) {
                	for (int i = 0; i < err_msg_array.size(); i++){
                		JSONObject object = (JSONObject) err_msg_array.get(i);
                		logger.error("Error " + object.get("code") + " : " + object.get("message"));
                    }
        		}
			} catch (ParseException e) {
				logger.error("Could not parse response from server: " + error, e);
			}
        }
        
        if (retval == EdgeCommunicator.EXCESSIVE_RATE) {
            try {
                logger.warn("Excessive rate detected. Sleeping for a minute...");
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                logger.warn("Exception while sleeping due to excessive rate", e);
            }
        }
        logger.debug("Returning retval");
        return retval;
    }

    private int encodeAndTransmit(String url, Message message) {
        JSONObject json = message.toJSON(true);
        String accessKey = (message.getAccessKey() == null) ? this.gateway.getAccessKey() : message.getAccessKey();
        String secretKey = (message.getSecretKey() == null) ? this.gateway.getSecretKey() : message.getSecretKey();
        String jsonStr = json.toJSONString();
        String hash = MessageUtils.encode(secretKey, jsonStr);
        return sendRequest("POST", url, jsonStr, accessKey, hash, message.getIsCompressed());
    }

    @Override
    public int connect() {
        return EdgeCommunicator.OK;
    }

    @Override
    public int transmit(Message msg) {
        switch (msg.getType()) {
            case Message.DATA:
            case Message.BULKDATA:
                return encodeAndTransmit(dataURL, msg);
            case Message.THING_REGISTER:
                return encodeAndTransmit(thingRegisterURL, msg);
            case Message.THING_HEARTBEAT:
                return encodeAndTransmit(thingHeartBeatURL, msg);
            case Message.ALERT:
                AlertMessage alert = (AlertMessage)msg;
                if (alert.getKey() == null) {
                    // Means we need to create a new Alert
                    return encodeAndTransmit(createAlertURL, alert);
                }
                JSONObject json = alert.toJSON();
                String jsonStr = json.toJSONString();
                String accessKey = (msg.getAccessKey() == null) ? this.gateway.getAccessKey() : msg.getAccessKey();
                String secretKey = (msg.getSecretKey() == null) ? this.gateway.getSecretKey() : msg.getSecretKey();
                String hash = MessageUtils.encode(secretKey, jsonStr);
                return sendRequest("PUT", EdgeUtil.getUpdateAlertUrl(apiHost, alert.getKey()), jsonStr, accessKey, hash, false);
        }
        return EdgeCommunicator.INVALID_REQUEST;
    }

    @Override
    public void shutdown() {
        // Not implemented
    }
}
