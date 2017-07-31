
package io.datonis.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors state of the gateway and keeps the connection alive with Datonis
 * 
 * @author Ranjit Nair (rnair@altizon.com)
 */
public class GatewayMonitor implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(GatewayMonitor.class);
    private EdgeGateway gateway;
    private long heartbeat;

    public GatewayMonitor(EdgeGateway agent) {
        this.gateway = agent;
        heartbeat = (Long)GatewayProperties.getValue(GatewayProperties.HEARTBEAT_INTERVAL);
    }

    @Override
    public void run() {

        logger.info("Gateway monitor starting up");
        boolean run = true;
        while (run) {
            try {
                if (!gateway.isRegistered()) {
                    logger.info("The gateway is not registered. Attempting to register Things");
                    if (!gateway.register()) {
                        logger.warn("Failed to register Things with the Datonis Platform. Will try again after " + heartbeat + " milliseconds");
                    }
                }
                if (gateway.isRegistered()) {
                    // Transmit a heart beat.
                    gateway.transmitHeartbeat();
                    logger.info("Sent heart beats for the Things");
                    Thread.sleep(heartbeat);
                } else {
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                if (gateway.isShutdown()) {
                    logger.info("Gateway monitor has been interrupted. Shutting down");
                    run = false;
                } else
                    logger.error("Gateway monitor has been interrupted", e);

            } catch (Exception e) {
                String errorMessage = "Gateway Monitor could not register Things with the Datonis Platform";
                if (gateway.isRegistered()) {
                    errorMessage = "Could not send heart beats for Things"; 
                }
                logger.error(errorMessage, e);
            }
        }
        logger.info("Gateway monitor has shut down");
    }
}
