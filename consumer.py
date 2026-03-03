#!/usr/bin/env python3
"""
Consumer node that receives messages via ZeroMQ.
"""
import zmq
import numpy as np
import pandas as pd
import time
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=3)
    parser.add_argument("--output_file", type=str, default="recv_times.csv")
    args = parser.parse_args()

    # Create ZeroMQ context and socket
    context = zmq.Context()

    socket = context.socket(zmq.PULL)
    socket.setsockopt(zmq.RCVHWM, 10)
    socket.setsockopt(zmq.RCVBUF, 10)


    socket.connect("ipc:///tmp/zeromq_comm.sock2")
    # socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print(f"Consumer starting... Listening for messages via IPC socket")
    process_times = []
    recv_times = []
    counter = 0
    try:
        # Receive 500 samples as warmup
        while counter < 500:
            # Receive message
            data = socket.recv()
            counter += 1

        counter = 0

        while counter < args.n_messages:
            # Receive message
            data = socket.recv()
            now = time.monotonic()
            timestamp = np.frombuffer(data, dtype=np.float64)[0]
            if counter == 0:
                first_timestamp = timestamp
            recv_times.append(now)
            process_times.append(now - timestamp)
            # print(f"Received message {counter+1}/{args.n_messages}: {timestamp}")
            counter += 1
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
        socket.close(linger=0)
        context.term()
    finally:
        print(f"Total messages processed: {len(process_times)}")
        recv_times_diff = np.diff(recv_times)
        mean_recv_period = np.mean(recv_times_diff)*1e6
        std_recv_period = np.std(recv_times_diff)*1e6
        max_recv_period = np.max(recv_times_diff)*1e6

        print(f"Average receive period (us): {mean_recv_period:.4f}")
        print(f"Max receive period (us): {max_recv_period:.4f}, idx: {np.argmax(recv_times_diff)}")
        print(f"Std receive period (us): {std_recv_period:.4f}")

        mean_latency = np.mean(process_times)*1e6
        std_latency = np.std(process_times)*1e6
        max_latency = np.max(process_times)*1e6

        print(f"Average latency (us): {mean_latency:.4f}")
        print(f"Max latency (us): {max_latency:.4f}, idx: {np.argmax(process_times)}")
        print(f"Std latency (us): {std_latency:.4f}")

        throughput = args.n_messages / (recv_times[-1] - first_timestamp)/1e3
        print(f"Started sending at {first_timestamp:.4f}, finished at {recv_times[-1]:.4f}")
        print(f"Throughput (msg/ms): {args.n_messages}/{(recv_times[-1] - first_timestamp)*1e3:.4f}ms={throughput:.2f}")

        results = pd.DataFrame({"Metric":["mean", "std", "max"],
                                "Recv_period": [mean_recv_period, std_recv_period, max_recv_period], 
                                "Latency": [mean_latency, std_latency, max_latency], 
                                "Throughput": [throughput, None, None]})

        results.to_csv(args.output_file, index=False)
        socket.close(linger=0)
        context.term()
        print("Consumer stopped.")

            

if __name__ == "__main__":
    main()
