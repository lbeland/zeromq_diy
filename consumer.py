#!/usr/bin/env python3
"""
Consumer node that receives messages via ZeroMQ.
"""
import zmq
import numpy as np
import time

def main():
    # Create ZeroMQ context and socket
    context = zmq.Context()

    socket = context.socket(zmq.PULL)
    socket.connect("ipc:///tmp/zeromq_comm.sock")
    # socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("Consumer starting... Listening for messages via IPC socket")
    process_times = []
    recv_times = []
    try:
        while True:
            # Receive message
            data = socket.recv()
            # print(f"Received: {data}")
            now = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
            recv_times.append(now)

            process_times.append(now - np.frombuffer(data, dtype=np.uint64)[0])
            
            # Process the data (example: just print it)
            # print(f"  -> Processing message ID {data['id']} with value {data['value']}")
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
        process_times = process_times[1:]     # exclude first measuremnt because of startup delay
        print(f"Total messages processed: {len(process_times)}")
        print(f"Average processing time (s): {np.mean(process_times)/1e9:.6f}")
        print(f"Max processing time (s): {np.max(process_times)/1e9:.6f}, idx: {np.argmax(process_times)}")
        print(f"Std processing time (s): {np.std(process_times)/1e9:.6f}")
        print('\n')
        recv_times = np.diff(recv_times)
        print(f"Average receive period (s): {np.mean(recv_times)/1e9:.6f}")
        print(f"Max receive period (s): {np.max(recv_times)/1e9:.6f}, idx: {np.argmax(recv_times)}")
        print(f"Std receive period (s): {np.std(recv_times)/1e9:.6f}")
    
    finally:
        socket.close()
        context.term()
        print("Consumer stopped.")

            

if __name__ == "__main__":
    main()
