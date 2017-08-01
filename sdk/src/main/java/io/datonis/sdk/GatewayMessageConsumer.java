
package io.datonis.sdk;

import io.datonis.sdk.communicator.EdgeCommunicator;
import io.datonis.sdk.communicator.MQTTCommunicator;
import io.datonis.sdk.message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes buffered messages and attempts to transmit them
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 * @author Rajesh Jangam (rajesh@altizon.com)
 */
public class GatewayMessageConsumer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(GatewayMessageConsumer.class);
    private final BlockingQueue<? extends Message> queue;
    private EdgeGateway agent;
    private ExecutorService executorService;
    private int threadPoolSize;
    private EdgeCommunicator communicator;
    private boolean skipRegisterCheck;
    private Long bulkInterval;

    public GatewayMessageConsumer(Long threadPoolSize, EdgeGateway agent, BlockingQueue<? extends Message> queue, EdgeCommunicator communicator, boolean skipRegister, Long bulkInterval) {
        this.agent = agent;
        this.queue = queue;
        this.threadPoolSize = threadPoolSize.intValue();
        this.communicator = communicator;
        this.skipRegisterCheck = skipRegister;
        this.bulkInterval = bulkInterval;
    }
    
    public GatewayMessageConsumer(Long threadPoolSize, EdgeGateway agent, BlockingQueue<? extends Message> queue, EdgeCommunicator communicator, Long bulkInterval) {
        this(threadPoolSize, agent, queue, communicator, false, bulkInterval);
    }
    
    private void shutdown() {
        try {
            logger.info(Thread.currentThread().getName() + " is waiting for the remaining messages to be sent");
            executorService.shutdownNow();
            executorService.awaitTermination(60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Issues while shutting down " + Thread.currentThread().getName(), e);
        }
    }
    
    protected Message getNextMessage(BlockingQueue<? extends Message> queue) throws InterruptedException {
        return queue.take();
    }

    @Override
    public void run() {
        final String currentThreadName = Thread.currentThread().getName();
        logger.info(currentThreadName + " is starting up");
        boolean run = true;
        final AtomicInteger threadNumber = new AtomicInteger();
        int poolSize = this.threadPoolSize;
        if (communicator instanceof MQTTCommunicator) {
            poolSize = 1;
        }
        executorService = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, currentThreadName + "-pool-thread-" + threadNumber.incrementAndGet());
            }
        });

        while (run) {
            try {
                if (agent.isShutdown()) {
                    run = false;
                    logger.info("Gateway seems to be shutdown. Stopping thread");
                    continue;
                }
                // Check if the agent is registered. if not, sleep for 15 seconds
                // and check again.
                if (skipRegisterCheck || agent.isRegistered()) {
                    // pull a message object out of the queue and deal with it by
                    // forking off a runnable object.
                    
                    //logger.info("Consumer thread sleeping");
                    //Thread.sleep(10000);
                    final Message message = getNextMessage(queue);
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                while (!communicator.isConnected()) {
                                    logger.info("Waiting for communicator to connect to the Datonis Server");
                                    Thread.sleep(50000);
                                }
                                logger.debug("Attempting to transmit next message");
                                long t1 = System.currentTimeMillis();
                                int transmitted = communicator.transmit(message);
                                logger.debug("Returned from communicator transmit");
                                if (transmitted == EdgeCommunicator.OK) {
                                    logger.debug("Before firing success callback");
    								fireSuccessfulMessageEvent(message);
    								logger.info("Transmitted Message in " + (System.currentTimeMillis() - t1) + " ms : " + message.toJSON(true));
                                } else {
    								fireFailedMessageEvent(message, transmitted);
                                    logger.warn("Failed to transmit message: " + message.toJSON(true) + ", Error: " +  EdgeUtil.getMappedErrorMessage(transmitted) + " [Code: " + transmitted + "]" );
                                }
                                if (bulkInterval != null) {
                                    logger.info("Thread sleeping for: " + bulkInterval + " ms due to bulk transmit configuration");
                                    Thread.sleep(bulkInterval);
                                }
                            } catch (Exception e) {
                                if (!agent.isShutdown()) {
                                    logger.error(Thread.currentThread().getName() + " hit an exception", e);
                                }
                            }
                        }
                    });
                } else {
                    logger.info("The agent is not registered. Messages will not be transmitted. Will check again in 15 seconds");
                    Thread.sleep(15000);
                }
            } catch (Exception e) {
                if (agent.isShutdown()) {
                    logger.info(Thread.currentThread().getName() + " has been interrupted. Shutting down");
                    run = false;
                } else {
                    logger.error(Thread.currentThread().getName() + " has received an error", e);
                }
            }
        }
        shutdown();
        logger.info(Thread.currentThread().getName() + " has been shut down");
    }

    private synchronized void fireFailedMessageEvent(Message message, int code) {
        agent.messageFailed(message, code);
    }

    private synchronized void fireSuccessfulMessageEvent(Message message) {
        logger.debug("Before calling message trasnmitted");
        agent.messageTransmitted(message);
    }
}
