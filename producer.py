#!/usr/bin/env python3
"""
Producer node that sends messages via ZeroMQ.
"""
import zmq
import time
import numpy as np
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=10)
    parser.add_argument("--freq", type=float, default=1000)
    parser.add_argument("--msg_size", type=int, default=1)
    parser.add_argument("--num_channels", type=int, default=1)
    parser.add_argument("--output_file", type=str, default="send_times.csv")
    args = parser.parse_args()


    # Create ZeroMQ context and socket
    context = zmq.Context()

    # PUB-SUB will generate samples even if no consumer is connected
    # PUSH-PULL requires at least one connected consumer to send messages
    socket = context.socket(zmq.PUSH)
    socket.bind("ipc:///tmp/zeromq_comm.sock")
    # socket.setsockopt(zmq.SNDHWM, 80)
    # socket.setsockopt(zmq.SNDBUF, 80)
    # print("Prod HWM: ",socket.get_hwm())
    
    print("Producer starting... Sending messages via IPC socket")
    
    send_delays = []
    send_times = []
    PERIOD_S = 1/args.freq
    try:

        
        default_data = np.ones((args.num_channels,args.msg_size), dtype=np.float64)
        socket.send(default_data.tobytes())

        next_deadline = time.monotonic()

        for i in range(args.n_messages):
            while True:
                now = time.monotonic()
                if now >= next_deadline:
                    data = default_data * now
                    socket.send(data.tobytes())
                    send_times.append(now)
                    # send_delays.append(np.float64(time.monotonic()) - now)
                    next_deadline += PERIOD_S
                    break


    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        print(f"Total messages sent: {len(send_times)}")
        # send_delays = send_delays[1:]
        # print(f"Average send time (s): {np.mean(send_delays)/1e9:.6f}")
        # print(f"Max send time (s): {np.max(send_delays)/1e9:.6f}, idx: {np.argmax(send_delays)}")
        # print(f"Std send time (s): {np.std(send_delays)/1e9:.6f}")
        # print('\n')

        # send_times = send_times[1:]
        send_times_diff = np.diff(send_times)
        print(f"Average send period (s): {np.mean(send_times_diff):.6f}")
        print(f"Max send period (s): {np.max(send_times_diff):.6f}, idx: {np.argmax(send_times_diff)}")
        print(f"Std send period (s): {np.std(send_times_diff):.6f}")
        save = np.vstack((send_times, np.append(send_times_diff, 0)))
        np.savetxt(args.output_file, save.T, delimiter=',', header='send_time,send_period')
if __name__ == "__main__":
    main()
