# Reliable Transport Protocol (4700send / 4700recv)

This project implements a reliable transport layer on top of UDP — delivering data in order, handling packet loss, and recovering from corruption.

## How it works

**Packet format** — I went with a binary format rather than something like JSON. It keeps overhead low and makes it easy to stay under the 1500-byte datagram limit. Data packets carry a type byte, a 4-byte sequence number, a 2-byte length field, the payload, and a 2-byte checksum at the end. ACKs are just type + ack number + checksum.

**Checksum** — Standard 16-bit one's-complement sum over the whole packet (with the checksum field zeroed out during computation). Any packet that fails the check gets silently dropped, both at the sender and receiver.

**Sender** — Sliding window approach. Reads from STDIN in chunks, slices them into segments of up to 1491 bytes, and sends them with sequence numbers. Unacknowledged segments are held in memory and retransmitted on timeout. Timeout is computed as estimated RTT + 4×RTT deviation, updated via exponential moving average. For congestion control: the cwnd starts at 2, grows exponentially during slow start, then switches to additive increase once it crosses ssthresh. On a timeout, ssthresh is halved and cwnd resets to 2. MAX_CWND is capped at 42 — enough to fill the pipe on high-bandwidth configs without blowing past the 64KB buffer limit.

**Receiver** — Cumulative ACK only (no selective ACK). Out-of-order segments are buffered in a `TreeMap` and flushed in order to STDOUT as gaps get filled. Every valid data segment — including duplicates - gets ACKed so the sender can detect loss properly.

## Challenges

The hardest part of this project was getting the last 3 (8 type) configs to pass. Performance was the main bottleneck — early implementations were either too conservative (timing out too slowly on low-latency links) or too aggressive (overflowing  queues on constrained ones).

A few specific pain points:

**Balancing level 1 vs. level 3** — Level 1 has a small router queue that overflows if you send aggressively; level 3 needs a window of at least 4 to pass. Starting with cwnd = 2 and growing slowly (roughly +1 per ACK) works for both.

**Level 6 (latency sensitivity)** — A fixed 1-second timeout was way too conservative on low-latency links: after a loss, the sender would just sit there waiting. 

**Checksum placement** — Placing the checksum last means one verification path handles both DATA and ACK packets, and we can validate before trusting the length field. Seemed cleaner than having separate logic per packet type.

## Design notes

- **Correctness**: Sequence numbers + cumulative ACK give in-order, duplicate-free delivery. Checksums reject corruption. Timeouts + retransmission handle loss.
- **Low overhead**: Binary headers are 7–9 bytes vs. the heavy JSON alternative.
- **Adaptability**: RTT estimation adjusts timeouts dynamically; the congestion window backs off on loss and grows on ACKs to handle a range of bandwidth/buffer conditions.

## Testing

**Full suite**: Run `./test` to run all 19 configs across levels 1–8. All should report `[PASS]`.

**Single config**: `./run configs/<config>.conf` runs one scenario and prints simulator logs plus final stats (success/failure, byte/packet counts).