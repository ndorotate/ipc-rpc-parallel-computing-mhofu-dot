package pdc;

import java.io.IOException;

/**
 * Interface for RPC method handlers.
 * Defines the contract for methods that can be invoked remotely.
 */
@FunctionalInterface
public interface RPCMethod {
    /**
     * Execute an RPC method with the given request payload.
     * 
     * @param requestPayload The serialized request data
     * @return The serialized response data
     * @throws IOException If communication fails
     * @throws Exception   If the method execution fails
     */
    byte[] execute(byte[] requestPayload) throws Exception;
}
