import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * Reliable transport sender: sliding window, timeout, RTT estimation, retransmission.
 * Reads data from STDIN, sends to recv_host:recv_port via UDP, exits when all data acked.
 */
public class Sender {
    private static final int MAX_DATA_LEN = Packet.MAX_DATA_LEN;

    private final String host;
    private final int port;
    private final DatagramChannel channel;
    private final Selector selector;
    private InetSocketAddress remoteAddress = null;

    /** Pending payloads to send (in order). Each is up to MAX_DATA_LEN bytes. */
    private final List<byte[]> segments = new ArrayList<>();
    private boolean eof = false;

    /** Sliding window: base = oldest unacked seq, nextSeqNum = next seq to use. */
    private int base = 0;
    private int nextSeqNum = 0;
    /** Unacked packets: seq -> payload (for retransmit). */
    private final Map<Integer, byte[]> unacked = new HashMap<>();

    /** Congestion window (packets). Start at 2 to avoid filling small buffers (level 1–2); grow for level 3+. */
    private int cwnd = 2;
    private static final int MIN_CWND = 2;
    /** Cap so in-flight bytes stay under 64KB buffer (8-2). */
    private static final int MAX_CWND = 42;
    /** Slow-start threshold: after timeout we back off and grow slowly (better for 8-1 with loss). */
    private int ssthresh = MAX_CWND;
    /** In congestion avoidance, count acks until we add 1 to cwnd (additive increase). */
    private int caAckCount = 0;

    /** RTT estimation (seconds). Start low to converge quickly on low-latency links. */
    private double estimatedRTT = 0.3;
    private double devRTT = 0.1;
    private static final double ALPHA = 0.125;
    private static final double BETA = 0.25;
    /** Timeout = estimatedRTT + 4*devRTT, with bounds. */
    private long timeoutMs = 700;
    private static final long MIN_TIMEOUT_MS = 150;
    private static final long MAX_TIMEOUT_MS = 10000;

    /** Send time for base packet (for RTT sample). */
    private long baseSendTime = 0;
    /** When we last sent the base packet (for timeout). */
    private long baseRetransmitTime = 0;
    private boolean timerRunning = false;

    private static final byte[] EOF_MARKER = new byte[0];
    private final BlockingQueue<byte[]> inputChunks = new LinkedBlockingQueue<>();
    private static final int POLL_MS = 10;

    public Sender(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        channel = DatagramChannel.open();
        channel.bind(new InetSocketAddress(0));
        channel.configureBlocking(false);
        selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);
        updateTimeout();
        log("Sender starting up, sending to " + host + ":" + port);
    }

    private void log(String message) {
        System.err.println(message);
        System.err.flush();
    }

    private void updateTimeout() {
        timeoutMs = (long) Math.min(MAX_TIMEOUT_MS, Math.max(MIN_TIMEOUT_MS, (estimatedRTT + 4 * devRTT) * 1000));
    }

    private void startInputThread() {
        Thread t = new Thread(() -> {
            try {
                byte[] buf = new byte[MAX_DATA_LEN * 2];
                InputStream in = System.in;
                while (true) {
                    int n = in.read(buf);
                    if (n == -1) {
                        inputChunks.put(EOF_MARKER);
                        break;
                    }
                    if (n > 0) {
                        inputChunks.put(Arrays.copyOf(buf, n));
                    }
                }
            } catch (IOException e) {
                log("Input error: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.setDaemon(true);
        t.start();
    }

    /** Drain input into segments (each up to MAX_DATA_LEN). */
    private void drainInput() {
        while (!eof) {
            byte[] chunk = inputChunks.poll();
            if (chunk == null) break;
            if (chunk == EOF_MARKER) {
                eof = true;
                break;
            }
            int offset = 0;
            while (offset < chunk.length) {
                int len = Math.min(MAX_DATA_LEN, chunk.length - offset);
                segments.add(Arrays.copyOfRange(chunk, offset, offset + len));
                offset += len;
            }
        }
    }

    private void sendSegment(int seq, byte[] payload) throws IOException {
        byte[] packet = Packet.makeData(seq, payload, 0, payload.length);
        if (remoteAddress == null) {
            remoteAddress = new InetSocketAddress(host, port);
        }
        channel.send(ByteBuffer.wrap(packet), remoteAddress);
        unacked.put(seq, payload);
        if (seq == base) {
            baseSendTime = System.currentTimeMillis();
            baseRetransmitTime = baseSendTime;
            timerRunning = true;
        }
    }

    private void receive() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(65535);
        SocketAddress addr = channel.receive(buffer);
        if (addr == null) return;
        if (remoteAddress == null) remoteAddress = (InetSocketAddress) addr;
        if (!addr.equals(remoteAddress)) {
            log("Received from unexpected remote; ignoring");
            return;
        }
        buffer.flip();
        int len = buffer.remaining();
        if (len == 0) return;
        byte[] bytes = new byte[len];
        buffer.get(bytes);

        int ackNum = Packet.parseAck(bytes, len);
        if (ackNum < 0) return;

        if (ackNum <= base) return;
        if (ackNum > base) {
            long now = System.currentTimeMillis();
            double sampleRTT = (now - baseSendTime) / 1000.0;
            if (ackNum == base + 1 && baseSendTime > 0) {
                estimatedRTT = (1 - ALPHA) * estimatedRTT + ALPHA * sampleRTT;
                devRTT = (1 - BETA) * devRTT + BETA * Math.abs(sampleRTT - estimatedRTT);
                updateTimeout();
            }
            for (int s = base; s < ackNum; s++) {
                unacked.remove(s);
            }
            int ackedCount = ackNum - base;
            base = ackNum;
            if (unacked.isEmpty()) {
                timerRunning = false;
            } else {
                baseSendTime = now;
                baseRetransmitTime = now;
            }
            if (cwnd < ssthresh) {
                cwnd = Math.min(MAX_CWND, cwnd + ackedCount);
            } else {
                caAckCount += ackedCount;
                if (caAckCount >= cwnd) {
                    cwnd = Math.min(MAX_CWND, cwnd + 1);
                    caAckCount = 0;
                }
            }
        }
    }

    private void checkTimeout() {
        if (!timerRunning || !unacked.containsKey(base)) return;
        long now = System.currentTimeMillis();
        if (now - baseRetransmitTime >= timeoutMs) {
            try {
                byte[] payload = unacked.get(base);
                if (payload != null) {
                    ssthresh = Math.max(MIN_CWND, cwnd / 2);
                    cwnd = MIN_CWND;
                    caAckCount = 0;
                    sendSegment(base, payload);
                    baseRetransmitTime = now;
                }
            } catch (IOException e) {
                log("Retransmit error: " + e.getMessage());
            }
        }
    }

    public void run() throws IOException {
        startInputThread();

        while (true) {
            drainInput();

            int ready = selector.select(POLL_MS);
            if (ready > 0) {
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (key.isReadable()) receive();
                }
            }

            checkTimeout();

            while (nextSeqNum < base + cwnd && nextSeqNum < segments.size()) {
                byte[] payload = segments.get(nextSeqNum);
                sendSegment(nextSeqNum, payload);
                nextSeqNum++;
            }

            if (eof && base >= segments.size() && unacked.isEmpty()) {
                log("All data sent and acked; exiting.");
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: 4700send <recv_host> <recv_port>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        try {
            Sender sender = new Sender(host, port);
            sender.run();
        } catch (IOException e) {
            System.err.println("Sender error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
