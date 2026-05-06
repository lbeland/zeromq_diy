import subprocess
import time
import os
import signal
import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
PRODUCER_ROOT = REPO_ROOT / "Producer"
CONSUMER_ROOT = REPO_ROOT / "Consumer"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=1000)
    parser.add_argument("--msg_size", type=int, default=1)
    parser.add_argument("--num_channels", type=int, default=1)
    parser.add_argument("--max_buffer_size", type=int, default=10)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--output_file", type=str, default="Producer.csv")
    args = parser.parse_args()

    result_folder = f"{REPO_ROOT}/rt_c_results"

    os.makedirs(result_folder, exist_ok=True)

    if os.path.exists(f"{result_folder}/{args.num_channels}_{args.msg_size}_Consumer.csv") and not args.overwrite:
        return

    # Remove stale IPC socket left by previously killed/root-owned runs.
    try:
        if os.path.exists("/tmp/zeromq_comm.sock"):
            os.remove("/tmp/zeromq_comm.sock")
    except OSError as exc:
        print(f"Failed to remove stale IPC socket: {exc}")
        return

    # 1. Start consumer FIRST
    consumer = subprocess.Popen(
        [   
            "sudo", "chrt", "-f", "99",
            "taskset", "-c", "5",
            f"{CONSUMER_ROOT}/consumer.bin",
            "--n_messages",  str(args.n_messages),
            "--max_buffer_size", str(args.max_buffer_size//2),
            "--output_file", f"{result_folder}/{args.num_channels}_{args.msg_size}"
        ],
        preexec_fn=os.setsid,  # allows group kill
    )

    # Give consumer time to bind socket
    time.sleep(0.5)

    # 2. Start producer
    producer = subprocess.Popen(
        [
            "sudo", "chrt", "-f", "99",
            "taskset", "-c", "6",
            f"{PRODUCER_ROOT}/producer.bin",
            "--n_messages", str(args.n_messages),
            "--num_channels", str(args.num_channels),
            "--msg_size", str(args.msg_size),
            "--max_buffer_size", str(args.max_buffer_size//2),
            "--output_file", f"{result_folder}/{args.num_channels}_{args.msg_size}"
        ],
        preexec_fn=os.setsid,
    )

    try:
        # 3. Wait for completion
        producer.wait(10)
        consumer.wait(10)
    except KeyboardInterrupt:
        print("Keyboard interrupt received, terminating processes")
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
