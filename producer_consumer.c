/*
 * Inproc Producer-Consumer Benchmark using ZeroMQ and pthreads.
 *
 * Producer and Consumer run as threads sharing the same ZMQ context,
 * communicating via inproc transport (zero-copy, no kernel crossing).
 *
 * Build:
 *   gcc -O2 -o benchmark_inproc benchmark_inproc.c -lzmq -lpthread -lm
 *
 * Usage:
 *   ./benchmark_inproc [options]
 *     --n_messages    <int>   Number of messages to exchange   (default: 1000)
 *     --msg_size      <int>   Doubles per channel              (default: 1)
 *     --num_channels  <int>   Number of channels               (default: 1)
 *     --max_buffer_size <int> ZMQ HWM                          (default: 10)
 *     --output_file   <str>   Base path for CSV output         (default: "result")
 */

#define _GNU_SOURCE
#include <zmq.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <math.h>
#include <getopt.h>
#include <errno.h>
#include <sched.h>

/* ------------------------------------------------------------------ */
/* Shared configuration passed to both threads                         */
/* ------------------------------------------------------------------ */
typedef struct {
    void   *context;
    int    *core_ids;        /* optional CPU core IDs for thread pinning */
    int     n_messages;
    int     msg_size;        /* doubles per channel */
    int     num_channels;
    int     max_buffer_size;
    float   t_wait;
    char    output_file[256];
} BenchmarkConfig;

/* ------------------------------------------------------------------ */
/* Thread result structs                                                */
/* ------------------------------------------------------------------ */
typedef struct {
    double *send_times;      /* absolute monotonic timestamps, length n */
    int     n;
} ProducerResult;

typedef struct {
    double *recv_times;      /* absolute monotonic timestamps, length n */
    double *process_times;   /* recv_time - msg_timestamp,    length n */
    int     n;
} ConsumerResult;

/* ------------------------------------------------------------------ */
/* Helper                                                               */
/* ------------------------------------------------------------------ */
static inline double get_monotonic_time(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

void pin_thread(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

/* ------------------------------------------------------------------ */
/* Producer thread                                                      */
/* ------------------------------------------------------------------ */
static void *producer_thread(void *arg) {
    BenchmarkConfig *cfg = (BenchmarkConfig *)arg;
    ProducerResult  *res = calloc(1, sizeof(ProducerResult));

    pin_thread(cfg->core_ids[0]);

    int message_size = cfg->num_channels * cfg->msg_size * (int)sizeof(double);

    /* --- socket setup --- */
    void *socket = zmq_socket(cfg->context, ZMQ_PUSH);
    if (!socket) {
        fprintf(stderr, "[Producer] zmq_socket failed: %s\n", zmq_strerror(errno));
        return res;
    }
    zmq_setsockopt(socket, ZMQ_SNDHWM, &cfg->max_buffer_size, sizeof(int));

    /* For inproc the binder must exist before the connector.
       We bind in the producer and connect in the consumer. */
    if (zmq_bind(socket, "inproc://benchmark") != 0) {
        fprintf(stderr, "[Producer] zmq_bind failed: %s\n", zmq_strerror(errno));
        zmq_close(socket);
        return res;
    }

    // printf("[Producer] Socket bound. Starting warmup...\n");


    /* --- allocate result storage --- */
    double *message_data = malloc(message_size);
    res->send_times = malloc(cfg->n_messages * sizeof(double));
    res->n          = 0;
    /* --- warmup --- */
    for (int i = 0; i < 500; i++) {
        double now = get_monotonic_time();
        for (int j = 0; j < cfg->num_channels * cfg->msg_size; j++)
            message_data[j] = now;
        if (zmq_send(socket, message_data, message_size, 0) == -1) {
            fprintf(stderr, "[Producer] warmup send error: %s\n", zmq_strerror(errno));
            zmq_close(socket);
            return res;
        }
    }
    // printf("[Producer] Warmup done. Sending %d messages...\n", cfg->n_messages);

    /* --- measurement loop --- */
    for (int i = 0; i < cfg->n_messages; i++) {
        if (cfg->t_wait > 0) {
            usleep(cfg->t_wait * 1e6);
        }
        double now = get_monotonic_time();
        for (int j = 0; j < cfg->num_channels * cfg->msg_size; j++)
            message_data[j] = now;
        if (zmq_send(socket, message_data, message_size, 0) == -1) {
            fprintf(stderr, "[Producer] send error at msg %d: %s\n", i, zmq_strerror(errno));
            break;
        }
        res->send_times[i] = now;
        res->n++;
    }
    // printf("[Producer] Sent %d messages.\n", res->n);

    /* --- statistics --- */
    int n = res->n;
    double sum_diff = 0.0, max_diff = 0.0, sum_sq_diff = 0.0;
    int max_idx = 0;
    for (int i = 0; i < n - 1; i++) {
        double diff = res->send_times[i + 1] - res->send_times[i];
        sum_diff    += diff;
        sum_sq_diff += diff * diff;
        if (diff > max_diff) { max_diff = diff; max_idx = i; }
    }
    double avg_s   = sum_diff / (n - 1);
    double avg_us  = avg_s * 1e6;
    double var     = (sum_sq_diff / (n - 1)) - (avg_s * avg_s);
    double std_us  = sqrt(fmax(0.0, var)) * 1e6;
    printf("[Producer] avg send period: %.3f µs  std: %.3f µs  max: %.3f µs (idx %d)\n",
           avg_us, std_us, max_diff * 1e6, max_idx);

    /* --- write CSVs --- */
    char path[512];
    snprintf(path, sizeof(path), "%s_Producer.csv", cfg->output_file);
    FILE *fp = fopen(path, "w");
    if (fp) {
        fprintf(fp, "Metric,Send_period\n");
        fprintf(fp, "mean,%.6f\n",  avg_us);
        fprintf(fp, "std,%.6f\n",   std_us);
        fprintf(fp, "max,%.6f\n",   max_diff * 1e6);
        fclose(fp);
    }

    snprintf(path, sizeof(path), "%s_send_times.csv", cfg->output_file);
    fp = fopen(path, "w");
    if (fp) {
        for (int i = 0; i < n - 1; i++)
            fprintf(fp, "%.17f\n", res->send_times[i + 1] - res->send_times[i]);
        fclose(fp);
    }

    zmq_close(socket);
    return res;
}

/* ------------------------------------------------------------------ */
/* Consumer thread                                                      */
/* ------------------------------------------------------------------ */
static void *consumer_thread(void *arg) {
    BenchmarkConfig *cfg = (BenchmarkConfig *)arg;
    ConsumerResult  *res = calloc(1, sizeof(ConsumerResult));

    pin_thread(cfg->core_ids[1]);

    /* inproc connect – socket must be created AFTER the producer binds.
       A short sleep ensures the bind completes first; in practice the
       pthread_create ordering already guarantees this, but be safe. */
    struct timespec delay = { .tv_sec = 0, .tv_nsec = 10 * 1000 * 1000 }; /* 10 ms */
    nanosleep(&delay, NULL);

    void *socket = zmq_socket(cfg->context, ZMQ_PULL);
    zmq_setsockopt(socket, ZMQ_RCVHWM, &cfg->max_buffer_size, sizeof(int));
    zmq_connect(socket, "inproc://benchmark");
    printf("[Consumer] Connected. Waiting for messages...\n");

    int expected_size = cfg->num_channels * cfg->msg_size * (int)sizeof(double);
    char *buf = malloc(expected_size);
    if (!buf) { zmq_close(socket); return res; }

    /* --- allocate result storage --- */
    res->recv_times    = malloc(cfg->n_messages * sizeof(double));
    res->process_times = malloc(cfg->n_messages * sizeof(double));
    res->n = 0;
    if (!res->recv_times || !res->process_times) {
        free(buf); zmq_close(socket); return res;
    }

    double first_timestamp = 0.0;

    /* --- warmup --- */
    for (int i = 0; i < 500; i++) {
        if (zmq_recv(socket, buf, expected_size, 0) == -1) {
            fprintf(stderr, "[Consumer] warmup recv error: %s\n", zmq_strerror(errno));
            free(buf); zmq_close(socket); return res;
        }
    }
    // printf("[Consumer] Warmup done. Receiving %d messages...\n", cfg->n_messages);

    /* --- measurement loop --- */
    while (res->n < cfg->n_messages) {
        // int size = zmq_recv(socket, buf, expected_size, ZMQ_DONTWAIT);
        // if (size == -1) {
        //     if (errno == EAGAIN) {
        //         continue;  // no message yet → spin
        //     }
        //     fprintf(stderr, "[Consumer] recv error: %s\n", zmq_strerror(errno));
        //     break;
        // }
        if (zmq_recv(socket, buf, expected_size, 0) == -1) {
            fprintf(stderr, "[Consumer] recv error at msg %d: %s\n",
                    res->n, zmq_strerror(errno));
            break;
        }
        double now = get_monotonic_time();
        double msg_ts;
        memcpy(&msg_ts, buf, sizeof(double));

        if (res->n == 0) first_timestamp = msg_ts;

        res->recv_times[res->n]    = now;
        res->process_times[res->n] = now - msg_ts;
        res->n++;
    }
    printf("[Consumer] Received %d messages.\n", res->n);

    int n = res->n;

    /* --- jitter statistics --- */
    double sum_diff = 0.0, max_diff = 0.0, sum_sq_diff = 0.0;
    int max_diff_idx = 0;
    for (int i = 0; i < n - 1; i++) {
        double diff = res->recv_times[i + 1] - res->recv_times[i];
        sum_diff    += diff;
        sum_sq_diff += diff * diff;
        if (diff > max_diff) { max_diff = diff; max_diff_idx = i; }
    }
    double avg_recv_s  = sum_diff / (n - 1);
    double avg_recv_us = avg_recv_s * 1e6;
    double var_recv    = (sum_sq_diff / (n - 1)) - (avg_recv_s * avg_recv_s);
    double std_recv_us = sqrt(fmax(0.0, var_recv)) * 1e6;
    printf("[Consumer] avg recv period: %.3f µs  std: %.3f µs  max: %.3f µs (idx %d)\n",
           avg_recv_us, std_recv_us, max_diff * 1e6, max_diff_idx);

    /* --- latency statistics --- */
    double sum_lat = 0.0, max_lat = 0.0, sum_sq_lat = 0.0;
    int max_lat_idx = 0;
    for (int i = 0; i < n; i++) {
        double l = res->process_times[i];
        sum_lat    += l;
        sum_sq_lat += l * l;
        if (l > max_lat) { max_lat = l; max_lat_idx = i; }
    }
    double avg_lat_s  = sum_lat / n;
    double avg_lat_us = avg_lat_s * 1e6;
    double var_lat    = (sum_sq_lat / n) - (avg_lat_s * avg_lat_s);
    double std_lat_us = sqrt(fmax(0.0, var_lat)) * 1e6;
    printf("[Consumer] avg latency: %.3f µs  std: %.3f µs  max: %.3f µs (idx %d)\n",
           avg_lat_us, std_lat_us, max_lat * 1e6, max_lat_idx);

    double elapsed_s   = res->recv_times[n - 1] - first_timestamp;
    double throughput  = n / elapsed_s / 1e3;   /* msg/ms */
    printf("[Consumer] throughput: %d / %.3f ms = %.2f msg/ms\n",
           n, elapsed_s * 1e3, throughput);

    /* --- write CSVs --- */
    char path[512];
    snprintf(path, sizeof(path), "%s_Consumer.csv", cfg->output_file);
    FILE *fp = fopen(path, "w");
    if (fp) {
        fprintf(fp, "Metric,Recv_period,Latency,Throughput\n");
        fprintf(fp, "mean,%.6f,%.6f,%.6f\n", avg_recv_us, avg_lat_us, throughput);
        fprintf(fp, "std,%.6f,%.6f, \n",      std_recv_us, std_lat_us);
        fprintf(fp, "max,%.6f,%.6f, \n",      max_diff * 1e6, max_lat * 1e6);
        fclose(fp);
    }

    snprintf(path, sizeof(path), "%s_recv_times.csv", cfg->output_file);
    fp = fopen(path, "w");
    if (fp) {
        for (int i = 0; i < n - 1; i++)
            fprintf(fp, "%.17f\n", res->recv_times[i + 1] - res->recv_times[i]);
        fclose(fp);
    }

    snprintf(path, sizeof(path), "%s_process_times.csv", cfg->output_file);
    fp = fopen(path, "w");
    if (fp) {
        for (int i = 0; i < n; i++)
            fprintf(fp, "%.17f\n", res->process_times[i]);
        fclose(fp);
    }

    free(buf);
    zmq_close(socket);
    return res;
}

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */
int main(int argc, char *argv[]) {
    BenchmarkConfig cfg = {
        .n_messages      = 1000,
        .msg_size        = 1,
        .num_channels    = 1,
        .max_buffer_size = 10,
        .t_wait          = 0.0001,
        .output_file     = "result"
    };

    static const struct option long_opts[] = {
        { "n_messages",      required_argument, 0, 'n' },
        { "msg_size",        required_argument, 0, 'm' },
        { "num_channels",    required_argument, 0, 'c' },
        { "max_buffer_size", required_argument, 0, 'b' },
        { "t_wait",          required_argument, 0, 't' },
        { "output_file",     required_argument, 0, 'o' },
        { 0, 0, 0, 0 }
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "n:m:c:b:t:o:", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'n': cfg.n_messages      = atoi(optarg); break;
            case 'm': cfg.msg_size        = atoi(optarg); break;
            case 'c': cfg.num_channels    = atoi(optarg); break;
            case 'b': cfg.max_buffer_size = atoi(optarg); break;
            case 't': cfg.t_wait          = atof(optarg); break;
            case 'o':
                strncpy(cfg.output_file, optarg, sizeof(cfg.output_file) - 1);
                cfg.output_file[sizeof(cfg.output_file) - 1] = '\0';
                break;
            default:
                fprintf(stderr, "Usage: %s [--n_messages N] [--msg_size M] "
                        "[--num_channels C] [--max_buffer_size B] "
                        "[--t_wait T] [--output_file PATH]\n", argv[0]);
                return 1;
        }
    }

    printf("Config: n_messages=%d  msg_size=%d  num_channels=%d  "
           "max_buffer_size=%d  t_wait=%f  output=%s\n",
           cfg.n_messages, cfg.msg_size, cfg.num_channels,
           cfg.max_buffer_size, cfg.t_wait, cfg.output_file);

    /* Shared ZMQ context – mandatory for inproc */
    cfg.context = zmq_ctx_new();
    if (!cfg.context) {
        fprintf(stderr, "zmq_ctx_new failed: %s\n", zmq_strerror(errno));
        return 1;
    }
    cfg.core_ids = (int[]){5,6};  /* optional: core IDs for producer and consumer */

    /* Launch threads.
       Producer binds first → start it before the consumer. */
    pthread_t prod_tid, cons_tid;
    pthread_create(&prod_tid, NULL, producer_thread, &cfg);
    pthread_create(&cons_tid, NULL, consumer_thread, &cfg);

    ProducerResult *prod_res = NULL;
    ConsumerResult *cons_res = NULL;
    pthread_join(prod_tid, (void **)&prod_res);
    pthread_join(cons_tid, (void **)&cons_res);

    /* Cleanup */
    zmq_ctx_destroy(cfg.context);

    if (prod_res) { free(prod_res->send_times);    free(prod_res); }
    if (cons_res) { free(cons_res->recv_times);
                    free(cons_res->process_times); free(cons_res); }

    printf("Benchmark complete.\n");
    return 0;
}
