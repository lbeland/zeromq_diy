import subprocess
import time
import os
import signal
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=1000)
    parser.add_argument("--msg_size", type=int, default=1)
    parser.add_argument("--num_channels", type=int, default=1)
    parser.add_argument("--output_file", type=str, default="Producer.csv")
    args = parser.parse_args()

    result_folder = "results"

    os.makedirs(result_folder, exist_ok=True)

    # if os.path.exists(f"{result_folder}/{args.num_channels}_{args.msg_size}_Consumer.csv"):
    #     return  # Skip if already exists

    os.remove("/tmp/zeromq_comm.sock") if os.path.exists("/tmp/zeromq_comm.sock") else None

    # 1. Start consumer FIRST
    consumer = subprocess.Popen(
        [
            "python3",
            "consumer.py",
            "--n_messages",  str(args.n_messages),
            "--output_file", f"{result_folder}/{args.num_channels}_{args.msg_size}_Consumer.csv"
        ],
        preexec_fn=os.setsid,  # allows group kill
    )

    # Give consumer time to bind socket
    time.sleep(0.5)

    # 2. Start producer
    producer = subprocess.Popen(
        [
            "python3",
            "producer.py",
            "--n_messages", str(args.n_messages),
            "--num_channels", str(args.num_channels),
            "--msg_size", str(args.msg_size),
            "--output_file", f"{result_folder}/{args.num_channels}_{args.msg_size}_Producer.csv"
        ],
        preexec_fn=os.setsid,
    )

    try:
        # 3. Wait for completion
        producer.wait(10)
        consumer.wait(10)
    except KeyboardInterrupt:
        terminate(producer)
        terminate(consumer)

    except subprocess.TimeoutExpired:
        print("Timeout expired, terminating processes")
        terminate(producer)
        terminate(consumer)
        raise

    print(f"Benchmark complete")


def terminate(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        pass


if __name__ == "__main__":
    main()
