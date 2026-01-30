import subprocess
import time
import os
import signal
import argparse

import psutil

def cpu_time_tree(proc):
    total = 0.0
    for p in [proc]:
        try:
            t = p.cpu_times()
            total += t.user + t.system
        except psutil.NoSuchProcess:
            pass
    return total


def main():
    this_pid = os.getpid()
    this_process = psutil.Process(this_pid)
    this_process.cpu_affinity([0])  # Pin to CPU 0

    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=30000)
    parser.add_argument("--freq", type=int, default=10000)
    parser.add_argument("--msg_size", type=int, default=1000)
    parser.add_argument("--num_channels", type=int, default=20)
    parser.add_argument("--output_file", type=str, default="send_times.csv")
    args = parser.parse_args()

    result_folder = "results_new"

    os.makedirs(result_folder, exist_ok=True)

    # if os.path.exists(f"{result_folder}/{args.freq}_{args.num_channels}_{args.msg_size}_recv_times.csv"):
    #     return  # Skip if already exists

    # 1. Start consumer FIRST

    consumer1 = subprocess.Popen(
        [
            "python3",
            "consumer1.py",
            "--n_messages",  str(args.n_messages),
            "--output_file", f"{result_folder}/{args.freq}_{args.num_channels}_{args.msg_size}_recv_times.csv"
        ],
        preexec_fn=os.setsid,  # allows group kill
    )
    cons1_p = psutil.Process(consumer1.pid)
    cons1_p.cpu_affinity([10])  # Pin to CPU 7

    consumer2 = subprocess.Popen(
        [
            "python3",
            "consumer2.py",
            "--n_messages",  str(args.n_messages),
            "--output_file", f"{result_folder}/{args.freq}_{args.num_channels}_{args.msg_size}_recv_times.csv"
        ],
        preexec_fn=os.setsid,  # allows group kill
    )
    cons2_p = psutil.Process(consumer2.pid)
    cons2_p.cpu_affinity([6])  # Pin to CPU 6
    

    # 2. Start producer
    producer = subprocess.Popen(
        [
            "python3",
            "producer.py",
            "--n_messages", str(args.n_messages),
            "--freq", str(args.freq),
            "--num_channels", str(args.num_channels),
            "--msg_size", str(args.msg_size),
            "--output_file", f"{result_folder}/{args.freq}_{args.num_channels}_{args.msg_size}_send_times.csv"
        ],
        preexec_fn=os.setsid,
    )
    prod_p = psutil.Process(producer.pid)
    prod_p.cpu_affinity([1])  # Pin to CPU 0
    

    try:
        # consumer2.wait(timeout=args.n_messages / args.freq + 20)
        # producer.wait(timeout=args.n_messages / args.freq + 10)
        # consumer1.wait(timeout=args.n_messages / args.freq + 10)


        # 3. Wait for completion and measure CPU time
        start_wall = time.monotonic()

        last_cpu = cpu_time_tree(prod_p) + cpu_time_tree(cons1_p)
        total_cpu = 0.0

        while consumer2.poll() is None:
            time.sleep(0.1)
            try:
                current_cpu = cpu_time_tree(cons1_p)
                total_cpu += current_cpu - last_cpu
                last_cpu = current_cpu
            except psutil.NoSuchProcess:
                break

        end_wall = time.monotonic()
        wall_time = end_wall - start_wall

        print(f"Total CPU time used: {total_cpu:.3f} s")
        print(f"Wall time: {wall_time:.3f} s")
        print(f"Average CPU utilization: {100 * total_cpu / wall_time:.1f}% of one core")
        print(f"Benchmark complete")
    except KeyboardInterrupt:
        print("Benchmark interrupted, terminating processes")
    except subprocess.TimeoutExpired:
        print("Timeout expired, terminating processes")
    finally:
        terminate(producer)
        terminate(consumer1)
        terminate(consumer2)

    


def terminate(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        pass


if __name__ == "__main__":
    main()
