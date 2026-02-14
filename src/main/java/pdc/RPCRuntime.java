package pdc;

import java.io.*;
import java.util.concurrent.*;

/**
 * RPCRuntime handles RPC method registration, dispatch, and execution.
 * Provides an abstraction layer over socket-based communication.
 */
public class RPCRuntime {
    private final ConcurrentHashMap<String, RPCMethod> registeredMethods = new ConcurrentHashMap<>();
    private final ExecutorService rpcExecutor;
    private final String runtimeName;

    public RPCRuntime(String runtimeName, int threadPoolSize) {
        this.runtimeName = runtimeName;
        this.rpcExecutor = Executors.newFixedThreadPool(threadPoolSize);
    }

    public RPCRuntime(String runtimeName) {
        this(runtimeName, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Register an RPC method that can be invoked remotely.
     * 
     * @param methodName The name of the method
     * @param method     The implementation of the method
     */
    public void registerMethod(String methodName, RPCMethod method) {
        registeredMethods.put(methodName, method);
        System.out.println("[" + runtimeName + "] Registered RPC method: " + methodName);
    }

    /**
     * Dispatch an RPC call by method name.
     * Executes the method asynchronously and returns a future.
     * 
     * @param methodName     The name of the method to invoke
     * @param requestPayload The request data
     * @return A future containing the response
     */
    public Future<byte[]> dispatchAsync(String methodName, byte[] requestPayload) {
        RPCMethod method = registeredMethods.get(methodName);
        if (method == null) {
            CompletableFuture<byte[]> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                    new RuntimeException("Method not found: " + methodName));
            return failedFuture;
        }

        return rpcExecutor.submit(() -> method.execute(requestPayload));
    }

    /**
     * Synchronous RPC call with timeout.
     * 
     * @param methodName     The name of the method to invoke
     * @param requestPayload The request data
     * @param timeoutSeconds Maximum time to wait for response
     * @return The response data
     * @throws Exception If method is not found or execution fails
     */
    public byte[] dispatch(String methodName, byte[] requestPayload, long timeoutSeconds) throws Exception {
        Future<byte[]> future = dispatchAsync(methodName, requestPayload);
        return future.get(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Check if a method is registered.
     */
    public boolean hasMethod(String methodName) {
        return registeredMethods.containsKey(methodName);
    }

    /**
     * Shutdown the runtime.
     */
    public void shutdown() {
        rpcExecutor.shutdown();
        try {
            if (!rpcExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                rpcExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            rpcExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get the runtime name.
     */
    public String getRuntimeName() {
        return runtimeName;
    }
}
