package pdc;

import java.io.*;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Custom Wire Format (Binary Protocol):
 * - magic (6 bytes): "CSM218"
 * - version (1 byte): protocol version
 * - messageType (4 bytes length + UTF string)
 * - studentId (4 bytes length + UTF string)
 * - timestamp (8 bytes): long
 * - payload (4 bytes length + binary data)
 */
public class Message {
    public String magic;
    public int version;
    public String messageType; // RPC message type
    public String studentId; // Student identifier
    public long timestamp;
    public byte[] payload;

    // Backward compatibility aliases
    public String type;
    public String sender;

    private static final String MAGIC_STRING = "CSM218";
    private static final int VERSION = 1;

    public Message() {
        this.magic = MAGIC_STRING;
        this.version = VERSION;
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this();
        this.messageType = messageType;
        this.studentId = studentId;
        this.type = messageType; // Backward compatibility
        this.sender = studentId; // Backward compatibility
        this.timestamp = System.currentTimeMillis();
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixed strings and binary data for framing.
     */
    public byte[] pack() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            // Write magic (6 bytes: "CSM218")
            dos.write(MAGIC_STRING.getBytes("UTF-8"));

            // Write version (1 byte)
            dos.writeByte(version);

            // Write messageType (length-prefixed string)
            byte[] messageTypeBytes = messageType != null ? messageType.getBytes("UTF-8") : new byte[0];
            dos.writeInt(messageTypeBytes.length);
            dos.write(messageTypeBytes);

            // Write studentId (length-prefixed string)
            byte[] studentIdBytes = studentId != null ? studentId.getBytes("UTF-8") : new byte[0];
            dos.writeInt(studentIdBytes.length);
            dos.write(studentIdBytes);

            // Write timestamp (8 bytes)
            dos.writeLong(timestamp);

            // Write payload (length-prefixed binary data)
            byte[] payloadBytes = payload != null ? payload : new byte[0];
            dos.writeInt(payloadBytes.length);
            dos.write(payloadBytes);

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        Message msg = new Message();

        // Read magic (6 bytes)
        byte[] magicBytes = new byte[6];
        dis.readFully(magicBytes);
        msg.magic = new String(magicBytes, "UTF-8");

        if (!msg.magic.equals(MAGIC_STRING)) {
            throw new IOException("Invalid magic: " + msg.magic);
        }

        // Read version (1 byte)
        msg.version = dis.readByte() & 0xFF;

        // Read messageType (length-prefixed string)
        int messageTypeLen = dis.readInt();
        byte[] messageTypeBytes = new byte[messageTypeLen];
        dis.readFully(messageTypeBytes);
        msg.messageType = new String(messageTypeBytes, "UTF-8");
        msg.type = msg.messageType; // Backward compatibility

        // Read studentId (length-prefixed string)
        int studentIdLen = dis.readInt();
        byte[] studentIdBytes = new byte[studentIdLen];
        dis.readFully(studentIdBytes);
        msg.studentId = new String(studentIdBytes, "UTF-8");
        msg.sender = msg.studentId; // Backward compatibility

        // Read timestamp (8 bytes)
        msg.timestamp = dis.readLong();

        // Read payload (length-prefixed binary data)
        int payloadLen = dis.readInt();
        byte[] payloadBytes = new byte[payloadLen];
        dis.readFully(payloadBytes);
        msg.payload = payloadBytes;

        return msg;
    }

    /**
     * Reads a single message from a socket input stream.
     * Handles the framing at the socket level.
     */
    public static Message readFromStream(DataInputStream dis) throws IOException {
        // Read magic (6 bytes)
        byte[] magicBytes = new byte[6];
        dis.readFully(magicBytes);
        String magic = new String(magicBytes, "UTF-8");

        if (!magic.equals(MAGIC_STRING)) {
            throw new IOException("Invalid magic: " + magic);
        }

        Message msg = new Message();
        msg.magic = magic;

        // Read version (1 byte)
        msg.version = dis.readByte() & 0xFF;

        // Read messageType (length-prefixed string)
        int messageTypeLen = dis.readInt();
        byte[] messageTypeBytes = new byte[messageTypeLen];
        dis.readFully(messageTypeBytes);
        msg.messageType = new String(messageTypeBytes, "UTF-8");
        msg.type = msg.messageType; // Backward compatibility

        // Read studentId (length-prefixed string)
        int studentIdLen = dis.readInt();
        byte[] studentIdBytes = new byte[studentIdLen];
        dis.readFully(studentIdBytes);
        msg.studentId = new String(studentIdBytes, "UTF-8");
        msg.sender = msg.studentId; // Backward compatibility

        // Read timestamp (8 bytes)
        msg.timestamp = dis.readLong();

        // Read payload (length-prefixed binary data)
        int payloadLen = dis.readInt();
        byte[] payloadBytes = new byte[payloadLen];
        dis.readFully(payloadBytes);
        msg.payload = payloadBytes;

        return msg;
    }

    /**
     * Writes a message to a socket output stream.
     */
    public void writeToStream(DataOutputStream dos) throws IOException {
        byte[] packed = this.pack();
        dos.write(packed);
        dos.flush();
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType='" + messageType + '\'' +
                ", studentId='" + studentId + '\'' +
                ", timestamp=" + timestamp +
                ", payloadLen=" + (payload != null ? payload.length : 0) +
                '}';
    }
}
