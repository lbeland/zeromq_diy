#!/usr/bin/env python3
"""
Producer node that sends messages via ZeroMQ.
"""
import zmq
import time
import random
import numpy as np

def main():
    # Create ZeroMQ context and socket
    context = zmq.Context()

    # PUB-SUB will generate samples even if no consumer is connected
    # PUSH-PULL requires at least one connected consumer to send messages
    socket = context.socket(zmq.PUSH)
    socket.bind("ipc:///tmp/zeromq_comm.sock")
    
    print("Producer starting... Sending messages via IPC socket")
    
    send_delays = []
    send_times = []
    PERIOD_NS = 1_000_000  # 1ms
    try:
        # while True:
            # # Generate some sample data
            # data = np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW))  # timestamp in ns

            # data = np.ones((1000,), dtype=np.uint64) * data
            
            # # Send the message
            # socket.send(data.tobytes())
            # # print(f"Sent: {data}")
            # send_times.append(np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)) - data[0])

        next_deadline = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)

        while True:
            now = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
            if now >= next_deadline:
                socket.send(np.uint64(now).tobytes())
                send_times.append(now)
                send_delays.append(np.uint64(time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)) - now)
                next_deadline += PERIOD_NS
            
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
        send_delays = send_delays[1:]
        print(f"Total messages sent: {len(send_delays)}")
        print(f"Average send time (s): {np.mean(send_delays)/1e9:.6f}")
        print(f"Max send time (s): {np.max(send_delays)/1e9:.6f}, idx: {np.argmax(send_delays)}")
        print(f"Std send time (s): {np.std(send_delays)/1e9:.6f}")
        print('\n')

        send_times = np.diff(send_times)
        print(f"Average send period (s): {np.mean(send_times)/1e9:.6f}")
        print(f"Max send period (s): {np.max(send_times)/1e9:.6f}, idx: {np.argmax(send_times)}")
        print(f"Std send period (s): {np.std(send_times)/1e9:.6f}")
    

if __name__ == "__main__":
    main()
