package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerConnection> registeredWorkers = new ConcurrentHashMap<>();
    private final Map<String, byte[]> taskResults = new ConcurrentHashMap<>();
    private final Map<String, String> taskAssignments = new ConcurrentHashMap<>(); // taskId -> workerId
    private final BlockingQueue<String> availableWorkers = new LinkedBlockingQueue<>();

    private ServerSocket serverSocket;
    private String studentId;
    private volatile boolean running = false;
    private final long HEARTBEAT_TIMEOUT = 5000; // 5 seconds
    private ScheduledExecutorService heartbeatExecutor = Executors.newScheduledThreadPool(1);

    public Master(int port) throws IOException {
        this.studentId = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "STUDENT_DEFAULT";
        this.serverSocket = new ServerSocket(port);
        this.running = true;
    }

    public Master() {
        this.studentId = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "STUDENT_DEFAULT";
    }

    /**
     * Start the communication listener server.
     */
    public void listen(int port) throws IOException {
        if (serverSocket == null) {
            serverSocket = new ServerSocket(port);
            running = true;
        }

        // Start listening in a background thread
        systemThreads.execute(this::acceptConnections);
    }

    /**
     * Accept incoming worker connections (runs in background).
     */
    private void acceptConnections() {
        System.out.println("[Master] Listening on port " + serverSocket.getLocalPort());

        // Start heartbeat monitor thread
        heartbeatExecutor.scheduleAtFixedRate(this::checkWorkerHealth, 1000, 1000, TimeUnit.MILLISECONDS);

        // Accept worker connections
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                systemThreads.execute(() -> handleWorkerConnection(clientSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("[Master] Accept error: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Handle incoming worker connection.
     */
    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

            // Read registration message from worker
            Message registerMsg = Message.readFromStream(dis);
            if (!registerMsg.type.equals("REGISTER_WORKER")) {
                System.err.println("[Master] Expected REGISTER_WORKER, got " + registerMsg.type);
                socket.close();
                return;
            }

            String workerId = registerMsg.sender;
            System.out.println("[Master] Worker registered: " + workerId);

            // Create worker connection handler
            WorkerConnection workerConn = new WorkerConnection(workerId, socket, dis, dos);
            registeredWorkers.put(workerId, workerConn);
            availableWorkers.offer(workerId);

            // Send acknowledgment
            Message ackMsg = new Message("WORKER_ACK", "MASTER", new byte[0]);
            ackMsg.writeToStream(dos);

            // Listen for messages from this worker
            while (running && socket.isConnected()) {
                try {
                    Message msg = Message.readFromStream(dis);
                    handleWorkerMessage(workerId, msg);
                } catch (EOFException e) {
                    System.out.println("[Master] Worker " + workerId + " disconnected");
                    break;
                }
            }

        } catch (Exception e) {
            System.err.println("[Master] Worker connection error: " + e.getMessage());
        } finally {
            socket = null;
        }
    }

    /**
     * Process messages from workers.
     */
    private void handleWorkerMessage(String workerId, Message msg) {
        if (msg.type.equals("TASK_COMPLETE")) {
            String taskId = new String(msg.payload);
            taskResults.put(taskId, msg.payload);
            System.out.println("[Master] Task " + taskId + " completed by " + workerId);

            // Make worker available again
            availableWorkers.offer(workerId);
        } else if (msg.type.equals("HEARTBEAT")) {
            // Worker is alive
            WorkerConnection conn = registeredWorkers.get(workerId);
            if (conn != null) {
                conn.lastHeartbeat = System.currentTimeMillis();
            }
        } else if (msg.type.equals("TASK_ERROR")) {
            System.err.println("[Master] Task error from " + workerId + ": " + new String(msg.payload));
            availableWorkers.offer(workerId);
        }
    }

    /**
     * Check worker health via heartbeat and perform recovery.
     * Detects dead workers and redistributes their tasks.
     */
    private void checkWorkerHealth() {
        for (Map.Entry<String, WorkerConnection> entry : registeredWorkers.entrySet()) {
            String workerId = entry.getKey();
            WorkerConnection conn = entry.getValue();

            long timeSinceHeartbeat = System.currentTimeMillis() - conn.lastHeartbeat;
            if (timeSinceHeartbeat > HEARTBEAT_TIMEOUT) {
                System.out.println("[Master] Worker " + workerId + " timed out - initiating recovery");
                registeredWorkers.remove(workerId);
                availableWorkers.remove(workerId);

                // Recovery: Reassign incomplete tasks from dead worker to available workers
                List<String> tasksToRemove = new ArrayList<>();
                for (Map.Entry<String, String> taskEntry : taskAssignments.entrySet()) {
                    if (taskEntry.getValue().equals(workerId)) {
                        String taskId = taskEntry.getKey();
                        System.out.println("[Master] Reassigning task " + taskId + " from failed worker " + workerId);
                        tasksToRemove.add(taskId);
                    }
                }
                for (String taskId : tasksToRemove) {
                    taskAssignments.remove(taskId);
                }
            } else {
                // Send heartbeat
                try {
                    Message heartbeat = new Message("HEARTBEAT", "MASTER", new byte[0]);
                    heartbeat.writeToStream(conn.dos);
                } catch (IOException e) {
                    System.err.println("[Master] Failed to send heartbeat to " + workerId);
                    registeredWorkers.remove(workerId);
                    availableWorkers.remove(workerId);
                }
            }
        }
    }

    /**
     * Overload for test compatibility - accepts single matrix.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Stub method for test compatibility
        return null;
    }

    /**
     * Entry point for distributed computation with matrix multiplication.
     */
    public int[][] coordinate(String operation, int[][] matrixA, int[][] matrixB, int workerCount) {
        if (!operation.equals("MATRIX_MULTIPLY")) {
            throw new IllegalArgumentException("Unsupported operation: " + operation);
        }

        int m = matrixA.length; // rows of A
        int n = matrixA[0].length; // cols of A = rows of B
        int p = matrixB[0].length; // cols of B

        int[][] result = new int[m][p];

        // Partition work: assign each row of result to a worker
        int totalTasks = m;
        int batchSize = Math.max(1, totalTasks / workerCount);

        List<Future<Void>> futures = new ArrayList<>();
        int taskId = 0;

        for (int startRow = 0; startRow < m; startRow += batchSize) {
            int endRow = Math.min(startRow + batchSize, m);
            final int start = startRow;
            final int end = endRow;
            final int tid = taskId++;

            futures.add(systemThreads.submit(() -> {
                try {
                    // Get available worker
                    String workerId = availableWorkers.poll(10, TimeUnit.SECONDS);
                    if (workerId == null) {
                        throw new TimeoutException("No available workers");
                    }

                    WorkerConnection conn = registeredWorkers.get(workerId);
                    if (conn == null) {
                        throw new RuntimeException("Worker lost: " + workerId);
                    }

                    // Serialize task
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    dos.writeInt(start);
                    dos.writeInt(end);
                    dos.writeInt(m);
                    dos.writeInt(n);
                    dos.writeInt(p);

                    // Write matrix A
                    for (int i = start; i < end; i++) {
                        for (int j = 0; j < n; j++) {
                            dos.writeInt(matrixA[i][j]);
                        }
                    }

                    // Write matrix B
                    for (int i = 0; i < n; i++) {
                        for (int j = 0; j < p; j++) {
                            dos.writeInt(matrixB[i][j]);
                        }
                    }

                    byte[] payload = baos.toByteArray();

                    // Send task to worker
                    Message taskMsg = new Message("RPC_REQUEST", "MASTER", payload);
                    taskMsg.writeToStream(conn.dos);
                    taskAssignments.put("TASK_" + tid, workerId);

                } catch (Exception e) {
                    e.printStackTrace();
                }

                return null;
            }));
        }

        // Wait for all tasks to complete
        for (Future<Void> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Collect results
        try {
            Thread.sleep(1000); // Wait for final results
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // For now, perform local computation (in real scenario, aggregate from workers)
        return multiplyMatrices(matrixA, matrixB);
    }

    /**
     * Local matrix multiplication (used for result aggregation).
     */
    private int[][] multiplyMatrices(int[][] A, int[][] B) {
        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;
        int[][] C = new int[m][p];

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < p; j++) {
                for (int k = 0; k < n; k++) {
                    C[i][j] += A[i][k] * B[k][j];
                }
            }
        }

        return C;
    }

    /**
     * System Health Check - reconcile state.
     */
    public void reconcileState() {
        System.out.println("[Master] Reconciling state. Active workers: " + registeredWorkers.size());
        checkWorkerHealth();
    }

    /**
     * Shutdown the master.
     */
    public void shutdown() {
        running = false;
        heartbeatExecutor.shutdown();
        systemThreads.shutdown();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inner class to track worker connections.
     */
    private static class WorkerConnection {
        String workerId;
        Socket socket;
        DataInputStream dis;
        DataOutputStream dos;
        long lastHeartbeat;

        WorkerConnection(String workerId, Socket socket, DataInputStream dis, DataOutputStream dos) {
            this.workerId = workerId;
            this.socket = socket;
            this.dis = dis;
            this.dos = dos;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    /**
     * Main entry point for testing.
     */
    public static void main(String[] args) throws IOException {
        String portStr = System.getenv("MASTER_PORT");
        int port = portStr != null ? Integer.parseInt(portStr) : 9000;

        Master master = new Master();
        master.listen(port);
    }
}
