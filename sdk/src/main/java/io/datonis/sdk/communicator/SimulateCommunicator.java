
package io.datonis.sdk.communicator;

import io.datonis.sdk.message.Message;

/**
 * Simulator that simply does not actually send data.
 * Useful for testing the SDK
 * 
 * @author Ranjit Nair (ranjit@altizon.com)
 *
 */
public class SimulateCommunicator implements EdgeCommunicator {

    @Override
    public int connect() {
        return EdgeCommunicator.OK;
    }

    @Override
    public int transmit(Message msg) {
        return EdgeCommunicator.OK;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public boolean isConnected() {
        return true;
    }
}
