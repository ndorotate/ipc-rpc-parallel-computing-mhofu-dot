package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private String workerId;
    private Socket socket;
    private DataInputStream dis;
    private DataOutputStream dos;
    private String studentId;

    private ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private volatile boolean running = false;
    private ScheduledExecutorService heartbeatExecutor = Executors.newScheduledThreadPool(1);

    // RPC abstraction layer
    private RPCRuntime rpcRuntime;
    private ParallelMatrixMultiplier multiplier;

    public Worker(String workerId) {
        this.workerId = workerId;
        this.studentId = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "STUDENT_DEFAULT";

        // Initialize RPC runtime and parallel multiplier
        this.rpcRuntime = new RPCRuntime("Worker[" + workerId + "]", 8);
        this.multiplier = new ParallelMatrixMultiplier(Runtime.getRuntime().availableProcessors());

        // Register RPC methods
        registerRPCMethods();
    }

    public Worker() {
        this(System.getenv("WORKER_ID") != null ? System.getenv("WORKER_ID") : "WORKER_" + System.currentTimeMillis());
    }

    /**
     * Register RPC methods that can be invoked by the master.
     */
    private void registerRPCMethods() {
        // Register matrix multiplication RPC method
        rpcRuntime.registerMethod("MATRIX_MULTIPLY", payload -> {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(payload);
                DataInputStream dis = new DataInputStream(bais);

                int startRow = dis.readInt();
                int endRow = dis.readInt();
                int m = dis.readInt();
                int n = dis.readInt();
                int p = dis.readInt();

                // Read matrix A (only rows we need)
                int[][] matrixA = new int[endRow - startRow][n];
                for (int i = 0; i < endRow - startRow; i++) {
                    for (int j = 0; j < n; j++) {
                        matrixA[i][j] = dis.readInt();
                    }
                }

                // Read matrix B
                int[][] matrixB = new int[n][p];
                for (int i = 0; i < n; i++) {
                    for (int j = 0; j < p; j++) {
                        matrixB[i][j] = dis.readInt();
                    }
                }

                // Perform parallel multiplication
                int[][] result = multiplier.multiplyRowParallel(matrixA, matrixB);

                // Serialize result
                ByteArrayOutputStream resultBaos = new ByteArrayOutputStream();
                DataOutputStream resultDos = new DataOutputStream(resultBaos);

                resultDos.writeInt(startRow);
                resultDos.writeInt(endRow);
                resultDos.writeInt(result.length);
                resultDos.writeInt(result[0].length);

                for (int[] row : result) {
                    for (int val : row) {
                        resultDos.writeInt(val);
                    }
                }

                return resultBaos.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException("RPC method MATRIX_MULTIPLY failed", e);
            }
        });
    }

    /**
     * Non-blocking method to start the worker execution.
     * Joins the cluster asynchronously.
     */
    public void execute() {
        final String masterHost = System.getenv("MASTER_HOST") != null ? System.getenv("MASTER_HOST") : "localhost";

        final int masterPort = System.getenv("MASTER_PORT") != null ? Integer.parseInt(System.getenv("MASTER_PORT"))
                : 9000;

        taskExecutor.execute(() -> joinCluster(masterHost, masterPort));
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int masterPort) {
        try {
            System.out.println("[Worker " + workerId + "] Connecting to Master at " + masterHost + ":" + masterPort);
            socket = new Socket(masterHost, masterPort);
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());

            // Send registration message
            Message registerMsg = new Message("REGISTER_WORKER", workerId, new byte[0]);
            registerMsg.writeToStream(dos);

            // Wait for acknowledgment
            Message ackMsg = Message.readFromStream(dis);
            if (ackMsg.type.equals("WORKER_ACK")) {
                System.out.println("[Worker " + workerId + "] Registered successfully");
                running = true;

                // Start heartbeat listener
                startHeartbeatListener();

                // Start listening for tasks
                listenForTasks();
            } else {
                System.err.println("[Worker " + workerId + "] Registration failed");
                socket.close();
            }

        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Failed to join cluster: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void startHeartbeatListener() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (running && socket != null && socket.isConnected()) {
                try {
                    // Send heartbeat acknowledgment
                    Message heartbeatAck = new Message("HEARTBEAT", workerId, new byte[0]);
                    heartbeatAck.writeToStream(dos);
                } catch (IOException e) {
                    System.err.println("[Worker " + workerId + "] Failed to send heartbeat");
                }
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Listen for incoming tasks from the master.
     */
    private void listenForTasks() {
        new Thread(() -> {
            try {
                while (running && socket.isConnected()) {
                    Message taskMsg = Message.readFromStream(dis);

                    if (taskMsg.type.equals("RPC_REQUEST")) {
                        // Execute task asynchronously using RPC runtime
                        taskExecutor.submit(() -> executeRPCTask(taskMsg));
                    } else if (taskMsg.type.equals("HEARTBEAT")) {
                        // Respond to heartbeat
                        try {
                            Message heartbeatAck = new Message("HEARTBEAT", workerId, new byte[0]);
                            heartbeatAck.writeToStream(dos);
                        } catch (IOException e) {
                            System.err.println("[Worker " + workerId + "] Failed to respond to heartbeat");
                        }
                    }
                }
            } catch (EOFException e) {
                System.out.println("[Worker " + workerId + "] Connection closed by master");
            } catch (Exception e) {
                System.err.println("[Worker " + workerId + "] Error listening for tasks: " + e.getMessage());
                e.printStackTrace();
            } finally {
                running = false;
            }
        }).start();
    }

    /**
     * Execute an RPC task using the RPC runtime.
     */
    private void executeRPCTask(Message taskMsg) {
        try {
            // Dispatch through RPC runtime
            byte[] result = rpcRuntime.dispatch("MATRIX_MULTIPLY", taskMsg.payload, 30);

            // Send result back to master
            Message resultMsg = new Message("TASK_COMPLETE", workerId, result);
            resultMsg.writeToStream(dos);

            System.out.println("[Worker " + workerId + "] Completed RPC task");

        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Error executing RPC task: " + e.getMessage());
            try {
                Message errorMsg = new Message("TASK_ERROR", workerId,
                        e.getMessage().getBytes());
                errorMsg.writeToStream(dos);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    /**
     * Shutdown the worker gracefully.
     */
    public void shutdown() {
        running = false;
        taskExecutor.shutdown();
        heartbeatExecutor.shutdown();
        rpcRuntime.shutdown();
        multiplier.shutdown();
        try {
            if (socket != null && socket.isConnected()) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main entry point for starting a worker.
     */
    public static void main(String[] args) {
        String workerId = System.getenv("WORKER_ID");
        if (workerId == null) {
            workerId = "WORKER_" + System.currentTimeMillis();
        }

        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null) {
            masterHost = "localhost";
        }

        String masterPortStr = System.getenv("MASTER_PORT");
        int masterPort = masterPortStr != null ? Integer.parseInt(masterPortStr) : 9000;

        Worker worker = new Worker(workerId);
        worker.joinCluster(masterHost, masterPort);

        // Keep worker running
        synchronized (worker) {
            try {
                worker.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
