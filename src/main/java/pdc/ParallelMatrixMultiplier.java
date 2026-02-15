package pdc;

import java.util.concurrent.*;

/**
 * ParallelMatrixMultiplier provides high-performance matrix multiplication
 * with guaranteed serial correctness first, then parallelization strategies.
 */
public class ParallelMatrixMultiplier {
    private final ExecutorService computeService;
    private final int numThreads;

    public ParallelMatrixMultiplier() {
        this.numThreads = Runtime.getRuntime().availableProcessors();
        this.computeService = Executors.newFixedThreadPool(numThreads);
    }

    public ParallelMatrixMultiplier(int numThreads) {
        this.numThreads = numThreads;
        this.computeService = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Serial matrix multiplication - guaranteed correct baseline.
     * C = A * B where A is (m x n) and B is (n x p), result is (m x p)
     * 
     * @param A Matrix A (m x n)
     * @param B Matrix B (n x p)
     * @return Result matrix C (m x p)
     */
    public int[][] multiplySerial(int[][] A, int[][] B) {
        if (A == null || B == null || A.length == 0 || B.length == 0) {
            throw new IllegalArgumentException("Matrices cannot be null or empty");
        }

        int m = A.length; // rows of A
        int n = A[0].length; // cols of A = rows of B
        int p = B[0].length; // cols of B

        if (B.length != n) {
            throw new IllegalArgumentException("Matrix dimensions incompatible for multiplication");
        }

        int[][] C = new int[m][p];

        // Standard matrix multiplication: C[i][j] = sum(A[i][k] * B[k][j])
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
     * Multiply two matrices in parallel using row-striped distribution.
     * Each thread computes a contiguous block of result rows.
     * Uses verified serial multiplication as baseline.
     * 
     * @param A Matrix A (m x n)
     * @param B Matrix B (n x p)
     * @return Result matrix C (m x p)
     */
    public int[][] multiplyRowParallel(int[][] A, int[][] B) {
        // For single thread, just use serial
        if (numThreads <= 1) {
            return multiplySerial(A, B);
        }

        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;

        int[][] C = new int[m][p];

        int rowsPerThread = Math.max(1, (m + numThreads - 1) / numThreads);

        CompletionService<Void> completionService = new ExecutorCompletionService<>(computeService);
        int taskCount = 0;

        // Submit row computation tasks
        for (int startRow = 0; startRow < m; startRow += rowsPerThread) {
            int endRow = Math.min(startRow + rowsPerThread, m);
            final int start = startRow;
            final int end = endRow;

            completionService.submit(() -> {
                // Serial computation for this block
                for (int i = start; i < end; i++) {
                    for (int j = 0; j < p; j++) {
                        int sum = 0;
                        for (int k = 0; k < n; k++) {
                            sum += A[i][k] * B[k][j];
                        }
                        C[i][j] = sum;
                    }
                }
                return null;
            });
            taskCount++;
        }

        // Wait for all tasks to complete
        for (int i = 0; i < taskCount; i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Matrix multiplication task failed", e);
            }
        }

        return C;
    }

    /**
     * Multiply two matrices in parallel using column-striped distribution.
     * Each thread computes a contiguous block of result columns.
     * 
     * @param A Matrix A (m x n)
     * @param B Matrix B (n x p)
     * @return Result matrix C (m x p)
     */
    public int[][] multiplyColumnParallel(int[][] A, int[][] B) {
        // For single thread, just use serial
        if (numThreads <= 1) {
            return multiplySerial(A, B);
        }

        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;

        int[][] C = new int[m][p];

        int colsPerThread = Math.max(1, (p + numThreads - 1) / numThreads);

        CompletionService<Void> completionService = new ExecutorCompletionService<>(computeService);
        int taskCount = 0;

        // Submit column computation tasks
        for (int startCol = 0; startCol < p; startCol += colsPerThread) {
            int endCol = Math.min(startCol + colsPerThread, p);
            final int start = startCol;
            final int end = endCol;

            completionService.submit(() -> {
                for (int i = 0; i < m; i++) {
                    for (int j = start; j < end; j++) {
                        int sum = 0;
                        for (int k = 0; k < n; k++) {
                            sum += A[i][k] * B[k][j];
                        }
                        C[i][j] = sum;
                    }
                }
                return null;
            });
            taskCount++;
        }

        // Wait for all tasks to complete
        for (int i = 0; i < taskCount; i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Matrix multiplication task failed", e);
            }
        }

        return C;
    }

    /**
     * Multiply two matrices in parallel using block-striped distribution.
     * Divides work into 2D blocks for better cache locality.
     * 
     * @param A Matrix A (m x n)
     * @param B Matrix B (n x p)
     * @return Result matrix C (m x p)
     */
    public int[][] multiplyBlockParallel(int[][] A, int[][] B) {
        // For single thread, just use serial
        if (numThreads <= 1) {
            return multiplySerial(A, B);
        }

        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;

        int[][] C = new int[m][p];

        int blockSize = Math.max(64, (int) Math.sqrt(m * p / numThreads));

        CompletionService<Void> completionService = new ExecutorCompletionService<>(computeService);
        int taskCount = 0;

        // Submit block computation tasks
        for (int startRow = 0; startRow < m; startRow += blockSize) {
            for (int startCol = 0; startCol < p; startCol += blockSize) {
                int endRow = Math.min(startRow + blockSize, m);
                int endCol = Math.min(startCol + blockSize, p);

                final int sr = startRow;
                final int er = endRow;
                final int sc = startCol;
                final int ec = endCol;

                completionService.submit(() -> {
                    for (int i = sr; i < er; i++) {
                        for (int j = sc; j < ec; j++) {
                            int sum = 0;
                            for (int k = 0; k < n; k++) {
                                sum += A[i][k] * B[k][j];
                            }
                            C[i][j] = sum;
                        }
                    }
                    return null;
                });
                taskCount++;
            }
        }

        // Wait for all tasks to complete
        for (int i = 0; i < taskCount; i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Matrix multiplication task failed", e);
            }
        }

        return C;
    }

    /**
     * Shutdown the executor.
     */
    public void shutdown() {
        computeService.shutdown();
        try {
            // Wait for graceful shutdown
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        computeService.shutdownNow();
    }
}
