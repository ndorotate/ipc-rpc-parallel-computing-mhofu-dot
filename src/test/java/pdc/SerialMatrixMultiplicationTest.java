package pdc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for serial matrix multiplication correctness.
 * Verifies baseline serial implementation before parallelization.
 */
class SerialMatrixMultiplicationTest {

    private ParallelMatrixMultiplier multiplier;

    @BeforeEach
    void setUp() {
        // Single-threaded multiplier for pure serial execution
        multiplier = new ParallelMatrixMultiplier(1);
    }

    @Test
    void testSerialMultiply_2x2_Identity() {
        // Test identity matrix: I * A = A
        int[][] identity = { { 1, 0 }, { 0, 1 } };
        int[][] A = { { 1, 2 }, { 3, 4 } };

        int[][] result = multiplier.multiplySerial(identity, A);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(2, result[0].length);

        // Verify: result should equal A
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                assertEquals(A[i][j], result[i][j], "Identity multiplication should preserve matrix");
            }
        }
    }

    @Test
    void testSerialMultiply_2x2_Basic() {
        // [[1,2],[3,4]] * [[5,6],[7,8]] = [[19,22],[43,50]]
        int[][] A = { { 1, 2 }, { 3, 4 } };
        int[][] B = { { 5, 6 }, { 7, 8 } };

        int[][] result = multiplier.multiplySerial(A, B);

        assertEquals(19, result[0][0], "result[0][0] should be 19");
        assertEquals(22, result[0][1], "result[0][1] should be 22");
        assertEquals(43, result[1][0], "result[1][0] should be 43");
        assertEquals(50, result[1][1], "result[1][1] should be 50");
    }

    @Test
    void testSerialMultiply_3x3() {
        // Standard 3x3 test
        int[][] A = {
                { 1, 2, 3 },
                { 4, 5, 6 },
                { 7, 8, 9 }
        };
        int[][] B = {
                { 9, 8, 7 },
                { 6, 5, 4 },
                { 3, 2, 1 }
        };

        int[][] result = multiplier.multiplySerial(A, B);

        // Manually calculated:
        // [0][0] = 1*9 + 2*6 + 3*3 = 9 + 12 + 9 = 30
        // [0][1] = 1*8 + 2*5 + 3*2 = 8 + 10 + 6 = 24
        // [0][2] = 1*7 + 2*4 + 3*1 = 7 + 8 + 3 = 18
        assertEquals(30, result[0][0]);
        assertEquals(24, result[0][1]);
        assertEquals(18, result[0][2]);

        // [1][0] = 4*9 + 5*6 + 6*3 = 36 + 30 + 18 = 84
        // [1][1] = 4*8 + 5*5 + 6*2 = 32 + 25 + 12 = 69
        // [1][2] = 4*7 + 5*4 + 6*1 = 28 + 20 + 6 = 54
        assertEquals(84, result[1][0]);
        assertEquals(69, result[1][1]);
        assertEquals(54, result[1][2]);
    }

    @Test
    void testSerialMultiply_RectangularMatrices() {
        // 3x2 * 2x3 = 3x3
        int[][] A = {
                { 1, 2 },
                { 3, 4 },
                { 5, 6 }
        };
        int[][] B = {
                { 1, 2, 3 },
                { 4, 5, 6 }
        };

        int[][] result = multiplier.multiplySerial(A, B);

        assertEquals(3, result.length, "Result should have 3 rows");
        assertEquals(3, result[0].length, "Result should have 3 columns");

        // [0][0] = 1*1 + 2*4 = 1 + 8 = 9
        // [0][1] = 1*2 + 2*5 = 2 + 10 = 12
        // [0][2] = 1*3 + 2*6 = 3 + 12 = 15
        assertEquals(9, result[0][0]);
        assertEquals(12, result[0][1]);
        assertEquals(15, result[0][2]);

        // [1][0] = 3*1 + 4*4 = 3 + 16 = 19
        // [1][1] = 3*2 + 4*5 = 6 + 20 = 26
        // [1][2] = 3*3 + 4*6 = 9 + 24 = 33
        assertEquals(19, result[1][0]);
        assertEquals(26, result[1][1]);
        assertEquals(33, result[1][2]);

        // [2][0] = 5*1 + 6*4 = 5 + 24 = 29
        // [2][1] = 5*2 + 6*5 = 10 + 30 = 40
        // [2][2] = 5*3 + 6*6 = 15 + 36 = 51
        assertEquals(29, result[2][0]);
        assertEquals(40, result[2][1]);
        assertEquals(51, result[2][2]);
    }

    @Test
    void testSerialMultiply_SingleRow() {
        // 1x3 * 3x1 = 1x1
        int[][] A = { { 1, 2, 3 } };
        int[][] B = { { 4 }, { 5 }, { 6 } };

        int[][] result = multiplier.multiplySerial(A, B);

        assertEquals(1, result.length);
        assertEquals(1, result[0].length);

        // [0][0] = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assertEquals(32, result[0][0]);
    }

    @Test
    void testSerialMultiply_SingleColumn() {
        // 3x1 * 1x3 = 3x3
        int[][] A = { { 1 }, { 2 }, { 3 } };
        int[][] B = { { 4, 5, 6 } };

        int[][] result = multiplier.multiplySerial(A, B);

        assertEquals(3, result.length);
        assertEquals(3, result[0].length);

        // First row: 1 * [4,5,6] = [4,5,6]
        assertEquals(4, result[0][0]);
        assertEquals(5, result[0][1]);
        assertEquals(6, result[0][2]);

        // Second row: 2 * [4,5,6] = [8,10,12]
        assertEquals(8, result[1][0]);
        assertEquals(10, result[1][1]);
        assertEquals(12, result[1][2]);

        // Third row: 3 * [4,5,6] = [12,15,18]
        assertEquals(12, result[2][0]);
        assertEquals(15, result[2][1]);
        assertEquals(18, result[2][2]);
    }

    @Test
    void testSerialMultiply_ZeroMatrix() {
        // Any matrix * zero matrix = zero matrix
        int[][] A = { { 1, 2 }, { 3, 4 } };
        int[][] B = { { 0, 0 }, { 0, 0 } };

        int[][] result = multiplier.multiplySerial(A, B);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                assertEquals(0, result[i][j], "Result should be zero matrix");
            }
        }
    }

    @Test
    void testSerialMultiply_LargeMatrix() {
        // Test with larger matrices to verify correctness at scale
        int size = 10;
        int[][] A = new int[size][size];
        int[][] B = new int[size][size];

        // Initialize with known values
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                A[i][j] = i + j + 1;
                B[i][j] = size - i - j;
            }
        }

        int[][] result = multiplier.multiplySerial(A, B);

        // Verify dimension
        assertEquals(size, result.length);
        assertEquals(size, result[0].length);

        // Spot check a few values
        // result[0][0] = sum(A[0][k] * B[k][0]) for k=0..9
        // = 1*10 + 2*9 + 3*8 + 4*7 + 5*6 + 6*5 + 7*4 + 8*3 + 9*2 + 10*1
        int expected = 0;
        for (int k = 0; k < size; k++) {
            expected += A[0][k] * B[k][0];
        }
        assertEquals(expected, result[0][0], "Large matrix[0][0] should be correct");
    }

    @Test
    void testParallelMatchesSerial_2x2() {
        // Verify that parallel matches serial result
        int[][] A = { { 1, 2 }, { 3, 4 } };
        int[][] B = { { 5, 6 }, { 7, 8 } };

        int[][] serial = multiplier.multiplySerial(A, B);
        int[][] parallel = multiplier.multiplyRowParallel(A, B);

        assertMatricesEqual(serial, parallel, "Parallel should match serial");
    }

    @Test
    void testParallelMatchesSerial_3x3() {
        int[][] A = {
                { 1, 2, 3 },
                { 4, 5, 6 },
                { 7, 8, 9 }
        };
        int[][] B = {
                { 9, 8, 7 },
                { 6, 5, 4 },
                { 3, 2, 1 }
        };

        int[][] serial = multiplier.multiplySerial(A, B);
        int[][] rowParallel = multiplier.multiplyRowParallel(A, B);
        int[][] colParallel = multiplier.multiplyColumnParallel(A, B);
        int[][] blockParallel = multiplier.multiplyBlockParallel(A, B);

        assertMatricesEqual(serial, rowParallel, "Row parallel should match serial");
        assertMatricesEqual(serial, colParallel, "Column parallel should match serial");
        assertMatricesEqual(serial, blockParallel, "Block parallel should match serial");
    }

    private void assertMatricesEqual(int[][] expected, int[][] actual, String message) {
        assertNotNull(actual, message + " - actual should not be null");
        assertEquals(expected.length, actual.length, message + " - row count mismatch");

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].length, actual[i].length,
                    message + " - column count mismatch at row " + i);

            for (int j = 0; j < expected[i].length; j++) {
                assertEquals(expected[i][j], actual[i][j],
                        message + " - value mismatch at [" + i + "][" + j + "]");
            }
        }
    }
}
