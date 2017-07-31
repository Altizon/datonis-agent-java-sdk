
package io.datonis.sdk.exception;

import io.datonis.sdk.Thing;

public class IllegalThingException extends Exception {
    private static final long serialVersionUID = 1L;
    private Thing thing;
    private String message;

    public IllegalThingException(Thing thing, String message) {
        super();
        this.thing = thing;
        this.message = message;
    }

    public Thing getThing() {
        return thing;
    }

    public String getMessage() {
        return message;
    }
}
