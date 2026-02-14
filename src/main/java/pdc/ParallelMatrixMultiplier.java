package pdc;

import java.util.concurrent.*;

/**
 * ParallelMatrixMultiplier provides high-performance matrix multiplication
 * using thread-based parallelization strategies.
 */
public class ParallelMatrixMultiplier {
    private final ExecutorService executor;
    private final int numThreads;

    public ParallelMatrixMultiplier() {
        this.numThreads = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(numThreads);
    }

    public ParallelMatrixMultiplier(int numThreads) {
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Multiply two matrices in parallel using row-striped distribution.
     * Each thread computes a contiguous block of result rows.
     * 
     * @param A Matrix A (m x n)
     * @param B Matrix B (n x p)
     * @return Result matrix C (m x p)
     */
    public int[][] multiplyRowParallel(int[][] A, int[][] B) {
        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;

        int[][] C = new int[m][p];

        int rowsPerThread = Math.max(1, (m + numThreads - 1) / numThreads);

        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
        int taskCount = 0;

        // Submit row computation tasks
        for (int startRow = 0; startRow < m; startRow += rowsPerThread) {
            int endRow = Math.min(startRow + rowsPerThread, m);
            final int start = startRow;
            final int end = endRow;

            completionService.submit(() -> {
                for (int i = start; i < end; i++) {
                    for (int j = 0; j < p; j++) {
                        for (int k = 0; k < n; k++) {
                            C[i][j] += A[i][k] * B[k][j];
                        }
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
        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;

        int[][] C = new int[m][p];

        int colsPerThread = Math.max(1, (p + numThreads - 1) / numThreads);

        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
        int taskCount = 0;

        // Submit column computation tasks
        for (int startCol = 0; startCol < p; startCol += colsPerThread) {
            int endCol = Math.min(startCol + colsPerThread, p);
            final int start = startCol;
            final int end = endCol;

            completionService.submit(() -> {
                for (int i = 0; i < m; i++) {
                    for (int j = start; j < end; j++) {
                        for (int k = 0; k < n; k++) {
                            C[i][j] += A[i][k] * B[k][j];
                        }
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
        int m = A.length;
        int n = A[0].length;
        int p = B[0].length;

        int[][] C = new int[m][p];

        int blockSize = Math.max(64, (int) Math.sqrt(m * p / numThreads));

        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
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
                            for (int k = 0; k < n; k++) {
                                C[i][j] += A[i][k] * B[k][j];
                            }
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
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
