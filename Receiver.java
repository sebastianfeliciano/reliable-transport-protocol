import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Reliable transport receiver: delivers data in-order, no duplicates, checksum validation.
 * First line to STDERR must be "Bound to port <port>".
 * Received data is printed to STDOUT only; all other output to STDERR.
 */
public class Receiver {
    private final DatagramChannel channel;
    private final Selector selector;
    private InetSocketAddress remoteAddress = null;

    /** Next sequence number we expect (cumulative ACK = this value). */
    private int nextExpected = 0;
    /** Out-of-order buffer: seq -> data. Deliver in order when nextExpected arrives. */
    private final TreeMap<Integer, byte[]> outOfOrder = new TreeMap<>();

    public Receiver() throws IOException {
        channel = DatagramChannel.open();
        channel.bind(new InetSocketAddress(0));
        channel.configureBlocking(false);
        selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);
        InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
        log("Bound to port " + local.getPort());
    }

    private void log(String message) {
        System.err.println(message);
        System.err.flush();
    }

    private void sendAck(int ackNum) throws IOException {
        if (remoteAddress == null) return;
        byte[] ack = Packet.makeAck(ackNum);
        channel.send(ByteBuffer.wrap(ack), remoteAddress);
    }

    private void receive() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(65535);
        SocketAddress addr = channel.receive(buffer);
        if (addr == null) return;
        if (remoteAddress == null) {
            remoteAddress = (InetSocketAddress) addr;
        }
        if (!addr.equals(remoteAddress)) {
            log("Error: received from unexpected remote; ignoring");
            return;
        }
        buffer.flip();
        int len = buffer.remaining();
        if (len == 0) return;
        byte[] bytes = new byte[len];
        buffer.get(bytes);

        Packet.DataPacket dp = Packet.parseData(bytes, len);
        if (dp == null) {
            return;
        }

        int seq = dp.seq;
        byte[] data = dp.data;

        if (seq < nextExpected) {
            sendAck(nextExpected);
            return;
        }
        if (seq == nextExpected) {
            try {
                System.out.write(data);
                System.out.flush();
            } catch (IOException e) {
                log("Write error: " + e.getMessage());
                return;
            }
            nextExpected++;
            while (outOfOrder.containsKey(nextExpected)) {
                byte[] buffered = outOfOrder.remove(nextExpected);
                try {
                    System.out.write(buffered);
                    System.out.flush();
                } catch (IOException e) {
                    log("Write error: " + e.getMessage());
                    return;
                }
                nextExpected++;
            }
            sendAck(nextExpected);
            return;
        }
        if (seq > nextExpected) {
            if (!outOfOrder.containsKey(seq)) {
                outOfOrder.put(seq, data);
            }
            sendAck(nextExpected);
        }
    }

    public void run() throws IOException {
        while (true) {
            selector.select();
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();
                if (key.isReadable()) {
                    receive();
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Receiver receiver = new Receiver();
            receiver.run();
        } catch (IOException e) {
            System.err.println("Receiver error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
