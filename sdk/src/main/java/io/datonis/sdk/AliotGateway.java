package io.datonis.sdk;

/**
 * @deprecated
 * Gateway interface for compatibility with older agents referring to this class
 * New interface is {@link EdgeGateway}
 * 
 * @author Rajesh Jangam (rajesh_jangam@altizon.com)
 *
 */
public class AliotGateway extends EdgeGateway {
    public AliotGateway() {
        super();
    }

    public AliotGateway(QueueFactory queueFactory) {
        super(queueFactory);
    }
}
