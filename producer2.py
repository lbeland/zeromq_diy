#!/usr/bin/env python3
"""
Producer node that sends messages via ZeroMQ.
"""
import zmq
import time
import numpy as np

def main():
    # Create ZeroMQ context and socket
    context = zmq.Context()

    # PUB-SUB will generate samples even if no consumer is connected
    # PUSH-PULL requires at least one connected consumer to send messages
    sender = context.socket(zmq.PUSH)
    sender.bind("ipc:///tmp/zeromq_comm.sock")
    # sender.setsockopt(zmq.SNDHWM, 2)  # set high water mark to 2
    # sender.setsockopt(zmq.SNDBUF, 20)

    
    print("Producer starting... Sending messages via IPC socket")
    
    send_delays = []
    send_times = []
    PERIOD_NS = 100_000_000  # 100ms
    try:
        # while True:
            # # Generate some sample data
            # data = np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW))  # timestamp in ns

            # data = np.ones((1000,), dtype=np.uint64) * data
            
            # # Send the message
            # sender.send(data.tobytes())
            # # print(f"Sent: {data}")
            # send_times.append(np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)) - data[0])

        now1 = time.time_ns()
        next_deadline = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
        now2 = time.time_ns()
        print("Overhead to get time:", now2 - now1, "ns")

        while True:
            now = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
            if now >= next_deadline:
                send_times.append(now)
                sender.send(np.uint64(now).tobytes(),flags=zmq.NOBLOCK)
                send_delays.append(np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)) - now)
                next_deadline += PERIOD_NS
    except zmq.error.Again:
        # Handle the situation where the socket is blocked due to the high water mark being reached
        print("Socket is blocked!")
        while True:
            # Check whether the socket is ready for sending
            # We use poll with a timeout of 5000ms to avoid busy-waiting
            if sender.poll(timeout=5000000, flags=zmq.POLLOUT):
                # If the socket is ready for sending, break out of the loop and try again
                print("Socket is ready again.")
                break
            else:
                # If the socket is not ready for sending, wait for the worker to consume messages
                print("Worker is slow! Waiting...")
                time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
        send_delays = send_delays[1:]
        print(f"Total messages sent: {len(send_delays)}")
        print(f"Average send time (s): {np.mean(send_delays)/1e9:.6f}")
        print(f"Max send time (s): {np.max(send_delays)/1e9:.6f}, idx: {np.argmax(send_delays)}")
        print(f"Std send time (s): {np.std(send_delays)/1e9:.6f}")
        print('\n')

        send_times = np.diff(send_times[1:])
        print(f"Average send jitter (s): {np.mean(send_times)/1e9:.6f}")
        print(f"Max send jitter (s): {np.max(send_times)/1e9:.6f}, idx: {np.argmax(send_times)}")
        print(f"Std send jitter (s): {np.std(send_times)/1e9:.6f}")
    
        sender.close()
        context.term()
        print("Producer stopped.")

if __name__ == "__main__":
    main()
