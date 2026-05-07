"""
Benchmark driver for benchmark_inproc.bin.

Because producer and consumer now live in the same process there is only one
binary to launch – no socket/timing race between two separate processes.

Usage:
    python inproc_benchmark.py [options]
"""

import subprocess
import os
import argparse
import signal
from pathlib import Path

REPO_ROOT   = Path(__file__).resolve().parent
BINARY_ROOT = REPO_ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages",      type=int,  default=1000)
    parser.add_argument("--msg_size",        type=int,  default=1)
    parser.add_argument("--num_channels",    type=int,  default=1)
    parser.add_argument("--max_buffer_size", type=int,  default=16)
    parser.add_argument("--t_wait",          type=float, default=0.0001)
    parser.add_argument("--overwrite",       action="store_true")
    args = parser.parse_args()

    result_folder = REPO_ROOT / "rt_c_results"
    result_folder.mkdir(exist_ok=True)

    consumer_csv = result_folder / f"{args.num_channels}_{args.msg_size}_Consumer.csv"
    if consumer_csv.exists() and not args.overwrite:
        print(f"Output already exists ({consumer_csv}), skipping. Use --overwrite to re-run.")
        return

    output_base = str(result_folder / f"{args.num_channels}_{args.msg_size}")

    proc = subprocess.Popen(
        [
            "sudo", "chrt", "-f", "99",
            "taskset", "-c", "0,1",          # allow both threads to use cores 5 & 6
            str(BINARY_ROOT / "producer_consumer.bin"),
            "--n_messages",      str(args.n_messages),
            "--msg_size",        str(args.msg_size),
            "--t_wait",          str(args.t_wait),
            "--num_channels",    str(args.num_channels),
            "--max_buffer_size", str(args.max_buffer_size),
            "--output_file",     output_base,
        ],
        preexec_fn=os.setsid,
    )

    try:
        proc.wait(timeout=30)
    except KeyboardInterrupt:
        print("Interrupted, terminating...")
        _terminate(proc)
    except subprocess.TimeoutExpired:
        print("Timeout, terminating...")
        _terminate(proc)
        raise

    print("Benchmark complete.")


def _terminate(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        pass


if __name__ == "__main__":
    main()
