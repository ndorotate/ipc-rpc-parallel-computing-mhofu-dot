package pdc;

import org.junit.jupiter.api.Test;
import java.io.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for basic RPC round-trip functionality.
 * Tests message protocol compliance and RPC method dispatch.
 */
class RpcRoundTripTest {

    @Test
    void testMessageProtocolCompliance() {
        Message testMsg = new Message("TEST_TYPE", "TEST_STUDENT", "TEST_PAYLOAD".getBytes());
        byte[] packed = testMsg.pack();
        String magic = new String(packed, 0, 6);
        assertEquals("CSM218", magic);
    }

    @Test
    void testMessageUnpack() throws IOException {
        Message original = new Message("TEST_TYPE", "TEST_STUDENT", "TEST_PAYLOAD".getBytes());
        byte[] packed = original.pack();
        Message unpacked = Message.unpack(packed);

        assertEquals("TEST_TYPE", unpacked.messageType);
        assertEquals("TEST_STUDENT", unpacked.studentId);
        assertArrayEquals("TEST_PAYLOAD".getBytes(), unpacked.payload);
    }

    @Test
    void testRpcMethodDispatch() throws Exception {
        RPCRuntime runtime = new RPCRuntime("TEST_RUNTIME");
        byte[] testData = "HELLO_WORLD".getBytes();
        runtime.registerMethod("ECHO", payload -> payload);

        byte[] result = runtime.dispatch("ECHO", testData, 5);
        assertArrayEquals(testData, result);
        runtime.shutdown();
    }

    @Test
    void testMatrixMultiplicationRPC() throws Exception {
        RPCRuntime runtime = new RPCRuntime("TEST_RUNTIME");
        ParallelMatrixMultiplier multiplier = new ParallelMatrixMultiplier(1);

        // Register matrix multiply method
        runtime.registerMethod("MATRIX_MULTIPLY", payload -> {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(payload);
                DataInputStream dis = new DataInputStream(bais);

                int startRow = dis.readInt();
                int endRow = dis.readInt();
                int m = dis.readInt();
                int n = dis.readInt();
                int p = dis.readInt();

                int[][] matrixA = new int[endRow - startRow][n];
                for (int i = 0; i < endRow - startRow; i++) {
                    for (int j = 0; j < n; j++) {
                        matrixA[i][j] = dis.readInt();
                    }
                }

                int[][] matrixB = new int[n][p];
                for (int i = 0; i < n; i++) {
                    for (int j = 0; j < p; j++) {
                        matrixB[i][j] = dis.readInt();
                    }
                }

                int[][] result = multiplier.multiplyRowParallel(matrixA, matrixB);

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
                throw new RuntimeException(e);
            }
        });

        // Create test matrices: 2x2 * 2x2
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(0); // startRow
        dos.writeInt(2); // endRow
        dos.writeInt(2); // m
        dos.writeInt(2); // n
        dos.writeInt(2); // p

        // Matrix A: [[1, 2], [3, 4]]
        dos.writeInt(1);
        dos.writeInt(2);
        dos.writeInt(3);
        dos.writeInt(4);

        // Matrix B: [[5, 6], [7, 8]]
        dos.writeInt(5);
        dos.writeInt(6);
        dos.writeInt(7);
        dos.writeInt(8);

        byte[] request = baos.toByteArray();
        byte[] responsePayload = runtime.dispatch("MATRIX_MULTIPLY", request, 5);

        assertNotNull(responsePayload);

        ByteArrayInputStream bais = new ByteArrayInputStream(responsePayload);
        DataInputStream dis = new DataInputStream(bais);

        int startRow = dis.readInt();
        int endRow = dis.readInt();
        int resultRows = dis.readInt();
        int resultCols = dis.readInt();

        assertEquals(0, startRow);
        assertEquals(2, endRow);
        assertEquals(2, resultRows);
        assertEquals(2, resultCols);

        int[][] result = new int[resultRows][resultCols];
        for (int i = 0; i < resultRows; i++) {
            for (int j = 0; j < resultCols; j++) {
                result[i][j] = dis.readInt();
            }
        }

        // Expected: [[19, 22], [43, 50]]
        assertEquals(19, result[0][0]);
        assertEquals(22, result[0][1]);
        assertEquals(43, result[1][0]);
        assertEquals(50, result[1][1]);

        runtime.shutdown();
        multiplier.shutdown();
    }

    @Test
    void testMessageStreamRoundTrip() throws IOException {
        Message original = new Message("STREAM_TEST", "STREAM_ID", "STREAM_DATA".getBytes());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.writeToStream(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        Message restored = Message.readFromStream(dis);

        assertEquals("STREAM_TEST", restored.messageType);
        assertEquals("STREAM_ID", restored.studentId);
        assertArrayEquals("STREAM_DATA".getBytes(), restored.payload);
    }
}
