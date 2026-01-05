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
    socket = context.socket(zmq.PUB)
    socket.bind("ipc:///tmp/zeromq_comm.sock")
    
    print("Producer starting... Sending messages via IPC socket")
    
    send_times = []
    try:
        while True:
            # Generate some sample data
            data = np.uint64(time.time_ns())  # timestamp in ns

            data = np.ones((1000,), dtype=np.uint64) * data
            
            # Send the message
            socket.send(data.tobytes())
            # print(f"Sent: {data}")
            send_times.append(np.uint64(time.time_ns()) - data[0])
            
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
        send_times = send_times[1:]
        print(f"Total messages sent: {len(send_times)}")
        print(f"Average send time (s): {np.mean(send_times)/1e9:.6f}")
        print(f"Max send time (s): {np.max(send_times)/1e9:.6f}, idx: {np.argmax(send_times)}")
        print(f"Std send time (s): {np.std(send_times)/1e9:.6f}")
    

if __name__ == "__main__":
    main()
