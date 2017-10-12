package io.datonis.sdk.exception;

public class EdgeGatewayException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -418230230608002489L;

    public EdgeGatewayException(String message) {
        super(message);
    }

    public EdgeGatewayException(String message, Exception e) {
        super(message, e);
    }
}
