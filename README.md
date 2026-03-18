# Reliable Transport Protocol (4700send / 4700recv)

## High-level approach

This project implements a UDP-based reliable transport with in-order, loss-tolerant, and corruption-tolerant delivery.

- **Packet format**: Binary (not JSON) to keep overhead low and stay under the 1500-byte datagram limit. Data packets: type (1), seq (4), data length (2), payload, checksum (2). ACK packets: type (1), ack number (4), checksum (2).
- **Checksum**: 16-bit one’s-complement sum over the packet (checksum field zeroed). Used to detect corruption (mangled packets); invalid packets are dropped at sender and receiver.
- **Sender**: Sliding window (Go-Back-N style). Reads STDIN in chunks, splits into segments of at most 1491 data bytes, sends with sequence numbers. Maintains unacked segments and retransmits on timeout. RTT is estimated with an exponential moving average; timeout = estimated RTT + 4×dev RTT (clamped). Congestion window starts at 2 (to avoid overfilling small buffers in level 1–2), grows on ACKs, and halves on timeout.
- **Receiver**: Cumulative ACK. Buffers out-of-order segments in a `TreeMap` and delivers in order to STDOUT. Sends ACK for every valid data segment (including duplicates) so the sender can detect loss. First line to STDERR is `Bound to port <port>` as required.

## Challenges

- **Level 1 vs level 3**: Level 1 needs a small window (e.g. 2) so the router queue does not overflow; level 3 needs a window of at least 4. Using an initial cwnd of 2 and allowing slow growth (e.g. +1 per ACK) lets both pass.
- **Level 6 (latency)**: With a fixed 1 s timeout, low-RTT links waited too long after losses. Using a lower initial RTT estimate (e.g. 0.3 s) and a smaller initial timeout (e.g. 700 ms) lets the sender converge quickly on low-latency configs while still behaving on high-latency ones.
- **Checksum placement**: Checksum is at the end of each packet so a single verification path works for both DATA and ACK and so we can verify before trusting the length field.

## Design properties

- **Correctness**: Sequence numbers and cumulative ACK give in-order, no-duplicate delivery; checksums reject corrupted packets; timeouts and retransmissions handle loss.
- **Low overhead**: Binary packets and 7–9 byte headers reduce bytes-on-the-wire compared to a JSON-based protocol.
- **Adaptability**: RTT estimation and timeouts adapt to delay; cwnd backs off on timeout and grows on ACKs to suit different bandwidth/buffer conditions.

## Testing

- **Automated**: Run `./test` (or `python3 test`) to exercise all provided configs (levels 1–8). All 19 configs should report `[PASS]`.
- **Single run**: `./run configs/<config>.conf` runs one scenario and prints simulator logs and final stats (success/failure and byte/packet counts).
- **Manual**: Start receiver (`./4700recv`), note the printed port, then run sender with that host/port and pipe input, e.g. `echo "hello" | ./4700send 127.0.0.1 <port>`.

No modifications were made to the provided `run` script or the files in `configs/`.
