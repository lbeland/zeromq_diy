#!/usr/bin/env python3
"""
Producer node that sends messages via ZeroMQ.
"""
import zmq
import time
import numpy as np
import pandas as pd
import argparse
import os
print("Producer scheduler params:", os.sched_getparam(0))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=3)
    parser.add_argument("--msg_size", type=int, default=1)
    parser.add_argument("--num_channels", type=int, default=1)
    parser.add_argument("--max_buffer_size", type=int, default=10)
    parser.add_argument("--output_file", type=str, default="producer_metric.csv")
    args = parser.parse_args()


    # Create ZeroMQ context and socket
    context = zmq.Context()

    # PUB-SUB will generate samples even if no consumer is connected
    # PUSH-PULL requires at least one connected consumer to send messages
    socket = context.socket(zmq.PUSH)
    socket.setsockopt(zmq.SNDHWM, args.max_buffer_size)
    print("Producer HWM: ",socket.get_hwm())

    socket.bind("ipc:///tmp/zeromq_comm.sock2")

    print(f"Producer starting... Sending messages via IPC socket")
    
    send_times = []
    try:
        default_data = np.ones((args.num_channels,args.msg_size), dtype=np.float64)
        

        # warmup loop to stabilize the stream before measurements
        for i in range(500):
            now = time.monotonic()
            # print(f"Sent warmup message {i+1}/5: {now}")
            socket.send((default_data*now).tobytes())

        # time.sleep(0.01)

        for i in range(args.n_messages):
            now = time.monotonic()
            data = default_data * now
            socket.send(data.tobytes()) # Blocks on full buffer

            # print(f"Sent message {i+1}/{args.n_messages}: {now}")
            send_times.append(now)

    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
        socket.close(linger=0)
        context.term()
    finally:
        print(f"Total messages sent: {len(send_times)}")

        send_times_diff = np.diff(send_times)
        mean_send_period = np.mean(send_times_diff)*1e6
        std_send_period = np.std(send_times_diff)*1e6
        max_send_period = np.max(send_times_diff)*1e6
        print(f"Average send period (us): {mean_send_period:.4f}")
        print(f"Max send period (us): {max_send_period:.4f}, idx: {np.argmax(send_times_diff)}")
        print(f"Std send period (us): {std_send_period:.4f}")

        results = pd.DataFrame({"Metric":["mean", "std", "max"],"Send_period": [mean_send_period, std_send_period, max_send_period]})
        results.to_csv(args.output_file + "_Producer.csv", index=False)

        send_times_diff.tofile(args.output_file + "_send_times.csv", sep="\n")
        print("Producer stopped.")

        
if __name__ == "__main__":
    # os.sched_setscheduler(0, os.SCHED_FIFO, os.sched_param(80))
    main()
