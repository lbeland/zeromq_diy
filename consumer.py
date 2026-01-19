#!/usr/bin/env python3
"""
Consumer node that receives messages via ZeroMQ.
"""
import zmq
import numpy as np
import time
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=1000)
    parser.add_argument("--output_file", type=str, default="recv_times.csv")
    args = parser.parse_args()

    # Create ZeroMQ context and socket
    context = zmq.Context()

    socket = context.socket(zmq.PULL)
    socket.connect("ipc:///tmp/zeromq_comm.sock")
    # socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print("Consumer starting... Listening for messages via IPC socket")
    process_times = []
    recv_times = []
    counter = 0
    try:
        while counter < args.n_messages:
            # Receive message
            data = socket.recv()
            # print(f"Received: {data}")
            now = time.monotonic()
            recv_times.append(now)

            process_times.append(now - np.frombuffer(data, dtype=np.float64)[0])
            counter += 1
            # Process the data (example: just print it)
            # print(f"  -> Processing message ID {data['id']} with value {data['value']}")
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    
    finally:
        print(f"Total messages processed: {len(process_times)}")
        process_times = process_times[1:]     # exclude first measuremnt because of startup delay
        # print(f"Average processing time (s): {np.mean(process_times)/1e9:.6f}")
        # print(f"Max processing time (s): {np.max(process_times)/1e9:.6f}, idx: {np.argmax(process_times)}")
        # print(f"Std processing time (s): {np.std(process_times)/1e9:.6f}")
        # print('\n')
        recv_times = recv_times[1:]
        recv_times_diff = np.diff(recv_times)
        print(f"Average receive period (s): {np.mean(recv_times_diff):.6f}")
        print(f"Max receive period (s): {np.max(recv_times_diff):.6f}, idx: {np.argmax(recv_times_diff)}")
        print(f"Std receive period (s): {np.std(recv_times_diff):.6f}")

        save = np.vstack((recv_times, process_times, np.append(recv_times_diff, 0)))
        np.savetxt(args.output_file, save.T, delimiter=',', header='recv_time,latency,recv_period')
        socket.close()
        context.term()
        print("Consumer stopped.")

            

if __name__ == "__main__":
    main()
