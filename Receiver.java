import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

public class Receiver {
    private final DatagramChannel channel;
    private final Selector selector;
    private InetSocketAddress remoteAddress = null;

    // Next sequence number we expect (cumulative ACK = this value).
    private int nextExpected = 0;
    // Out-of-order buffer: seq -> data. Deliver in order when nextExpected arrives.
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

    // Send an ACK to the sender
    private void sendAck(int ackNum) throws IOException {
        if (remoteAddress == null) return;
        byte[] ack = Packet.makeAck(ackNum);
        channel.send(ByteBuffer.wrap(ack), remoteAddress);
    }

    // Receive a DATA packet from the sender
    private void receive() throws IOException {
        // Create a buffer to receive the packet
        ByteBuffer buffer = ByteBuffer.allocate(65535);
        SocketAddress addr = channel.receive(buffer);
        if (addr == null) return;
        // Set the remote address if it's not set
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

        // Parse the DATA packet
        Packet.DataPacket dp = Packet.parseData(bytes, len);
        if (dp == null) {
            return;
        }

        // Get the sequence number and data from the DATA packet
        int seq = dp.seq;
        byte[] data = dp.data;

        // If the sequence number is less than the next expected sequence number, send an ACK and return
        if (seq < nextExpected) {
            sendAck(nextExpected);
            return;
        }
        // If the sequence number is the same as nextExpected, write the data to stdout and send an ACK
        if (seq == nextExpected) {
            try {
                System.out.write(data);
                System.out.flush();
            } catch (IOException e) {
                log("Write error: " + e.getMessage());
                return;
            }
            nextExpected++;
            // Write any buffered data to stdout and send an ACK
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
            // Send an ACK for the next expected sequence number
            sendAck(nextExpected);
            return;
        }
        // If the sequence number is greater than nextExpected, add the data to the out-of-order buffer and send an ACK
        if (seq > nextExpected) {
            if (!outOfOrder.containsKey(seq)) {
                outOfOrder.put(seq, data);
            }
            sendAck(nextExpected);
        }
    }

    // Run the receiver
    public void run() throws IOException {
        // Select the channel for reading
        while (true) {
            selector.select();
            // Get the iterator for the selected keys
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            // Iterate through the selected keys
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                // Remove the key from the iterator
                iter.remove();
                // If the key is readable, receive the data
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
