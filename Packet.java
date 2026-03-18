import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * DATA: type(1)=0, seq(4), dataLen(2), data[], checksum(2) at end
 * ACK:  type(1)=1, ack(4), checksum(2)
 * Max data payload = 1500 - 9 = 1491 bytes.
 */

public class Packet {
    
    public static final int TYPE_DATA = 0;
    public static final int TYPE_ACK = 1;

    public static final int MAX_DATA_LEN = 1491;
    public static final int DATA_HEADER_LEN = 7;   // type + seq + dataLen (no checksum in header)
    public static final int ACK_PACKET_LEN = 7;

    // Data packet
    public static class DataPacket {
        public final int seq;
        public final byte[] data;

        DataPacket(int seq, byte[] data) {
            this.seq = seq;
            this.data = data;
        }
    }

    // Make a DATA packet
    public static byte[] makeData(int seq, byte[] data, int offset, int length) {
        int total = DATA_HEADER_LEN + length + 2;
        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) TYPE_DATA);
        buf.putInt(seq);
        buf.putShort((short) length);
        buf.put(data, offset, length);
        buf.putShort((short) 0);
        buf.flip();
        short csum = checksum(buf);
        buf.putShort(DATA_HEADER_LEN + length, csum);
        return buf.array();
    }

    // Make an ACK packet
    public static byte[] makeAck(int ackNum) {
        ByteBuffer buf = ByteBuffer.allocate(ACK_PACKET_LEN);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) TYPE_ACK);
        buf.putInt(ackNum);
        buf.putShort((short) 0);
        buf.flip();
        short csum = checksum(buf);
        buf.putShort(5, csum);
        return buf.array();
    }

    // 16-bit one's complement sum (same idea as TCP/UDP).
    public static short checksum(ByteBuffer buf) {
        int sum = 0;
        int pos = buf.position();
        int limit = buf.limit();
        // Sum the bytes in the buffer
        while (pos + 1 < limit) {
            sum += ((buf.get(pos) & 0xFF) << 8) | (buf.get(pos + 1) & 0xFF);
            pos += 2;
        }
        // If there is an odd number of bytes, add the last byte
        if (pos < limit) {
            sum += (buf.get(pos) & 0xFF) << 8;
        }
        // If the sum is greater than 16 bits, add the carry
        while ((sum >>> 16) != 0) {
            sum = (sum & 0xFFFF) + (sum >>> 16);
        }
        return (short) ~sum;
    }

    // Checksum is always in the last 2 bytes of the packet.
    public static boolean verifyChecksum(byte[] bytes, int length) {
        if (length < 2) return false;
        short stored = (short) (((bytes[length - 2] & 0xFF) << 8) | (bytes[length - 1] & 0xFF));
        // Set the checksum to 0
        bytes[length - 2] = 0;
        bytes[length - 1] = 0;
        // Create a buffer from the bytes
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, length).order(ByteOrder.BIG_ENDIAN);
        short computed = checksum(buf);
        return stored == computed;
    }

    // Parse DATA packet; returns null if invalid.
    public static DataPacket parseData(byte[] bytes, int length) {
        if (length < DATA_HEADER_LEN + 2 || bytes[0] != TYPE_DATA) return null;
        int dataLen = ((bytes[5] & 0xFF) << 8) | (bytes[6] & 0xFF);
        // If the length is not valid, return null
        if (length != DATA_HEADER_LEN + dataLen + 2) return null;
        // If the checksum is not valid, return null    
        if (!verifyChecksum(bytes, length)) return null;
        // Parse the sequence number
        int seq = ((bytes[1] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16) | ((bytes[3] & 0xFF) << 8) | (bytes[4] & 0xFF);
        // Create a new byte array for the data
        byte[] data = new byte[dataLen];
        System.arraycopy(bytes, 7, data, 0, dataLen);
        return new DataPacket(seq, data);
    }

    // Parse ACK packet; returns -1 if invalid.
    public static int parseAck(byte[] bytes, int length) {
        if (length != ACK_PACKET_LEN || bytes[0] != TYPE_ACK) return -1;
        if (!verifyChecksum(bytes, length)) return -1;
        // Parse the ACK number
        return ByteBuffer.wrap(bytes, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt();
    }

}
