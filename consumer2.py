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

    receiver = context.socket(zmq.PULL)
    receiver.connect("ipc:///tmp/zeromq_comm.sock")
    # receiver.setsockopt(zmq.RCVHWM, 2)  # set high water mark to 2
    # receiver.setsockopt(zmq.RCVBUF, 20)
    # socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("Consumer starting... Listening for messages via IPC socket")
    process_times = []
    recv_times = []
    try:
        while True:
            # Receive message
            data = receiver.recv()
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
        recv_times = np.diff(recv_times[1:])
        print(f"Average receive jitter (s): {np.mean(recv_times)/1e9:.6f}")
        print(f"Max receive jitter (s): {np.max(recv_times)/1e9:.6f}, idx: {np.argmax(recv_times)}")
        print(f"Std receive jitter (s): {np.std(recv_times)/1e9:.6f}")
    
    finally:
        receiver.close()
        context.term()
        print("Consumer stopped.")

            

if __name__ == "__main__":
    main()
