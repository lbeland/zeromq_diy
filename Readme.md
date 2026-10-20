# zeromq_diy — IPC benchmark

Origin: self-made, ZeroMQ-based reference implementation.

A minimal producer/consumer benchmark built directly on
[ZeroMQ](https://zeromq.org/) sockets, used as a low-level reference point
against the heavier frameworks in this repo. Two variants:

- `benchmark.py` — pure-Python producer/consumer (`producer_block.py`/`Producer/producer.py`).
- `c_benchmark.py` — C producer/consumer (`producer_consumer.c`, `Producer/producer.c`).

## Install

From the repo root:

```bash
./bootstrap.sh --only zeromq_diy                  # venv + Python deps
./bootstrap.sh --only zeromq_diy --with-builds --with-system-deps  # + C binaries (needs libzmq3-dev)
```

## Run the benchmark

Python variant:

```bash
cd zeromq_diy
venv/bin/python3 benchmark.py \
  --n_messages 1000 \
  --num_channels 1 \
  --msg_size 1 \
  --t_wait 0.0001 \
  --overwrite
```

C variant (requires `--with-builds` first):

```bash
cd zeromq_diy
venv/bin/python3 c_benchmark.py \
  --n_messages 1000 \
  --num_channels 1 \
  --msg_size 1 \
  --t_wait 0.0001 \
  --overwrite
```

- `--n_messages`: number of timed messages sent
- `--num_channels` / `--msg_size`: shape of each message
- `--t_wait`: seconds the producer sleeps between sends (0 = no wait)
- `--overwrite`: rerun even if results for this channel/size combination
  already exist

Python-variant results are written to
`rt_results/<num_channels>_<msg_size>_{Producer,Consumer}.csv`, C-variant
results to `rt_c_results/` in the same naming scheme.
