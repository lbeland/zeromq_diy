/*
 * Consumer node that receives messages via ZeroMQ.
 */
#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <getopt.h>

double get_monotonic_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

int main(int argc, char *argv[]) {
    int n_messages = 1000;
    char output_file[256] = "recv_times.csv";

    // Parse command-line arguments
    int opt;
    static const struct option long_options[] =
    {
        { "n_messages", required_argument, 0, 'n' },
        { "output_file", required_argument, 0, 'o' },
        0
    };
    while ((opt = getopt_long(argc, argv, "n:o:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'n':
                n_messages = atoi(optarg);
                break;
            case 'o':
                strncpy(output_file, optarg, sizeof(output_file) - 1);
                output_file[sizeof(output_file) - 1] = '\0';
                break;
            default:
                fprintf(stderr, "Usage: %s [options]\n", argv[0]);
                fprintf(stderr, "  -n n_messages (default: 1000)\n");
                fprintf(stderr, "  -o output_file (default: recv_times.csv)\n");
                return 1;
        }
    }

    // Create ZeroMQ context and socket
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PULL);
    zmq_connect(socket, "ipc:///tmp/zeromq_comm.sock");

    printf("Consumer starting... Listening for messages via IPC socket\n");

    // Allocate arrays for tracking times
    double *recv_times = (double *)malloc(n_messages * sizeof(double));
    double *process_times = (double *)malloc(n_messages * sizeof(double));
    double *recv_times_diff = (double *)malloc((n_messages - 1) * sizeof(double));

    if (!recv_times || !process_times || !recv_times_diff) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }

    int receive_count = 0;

    // Allocate buffer for receiving messages
    // We'll use a large enough buffer; ZeroMQ will tell us the actual size
    int EXPECTED_SIZE = 40 * 4096 * 8;  // max expected message size (num_channels * msg_size * sizeof(double))
    char message_buffer[EXPECTED_SIZE];  // max expected message size (num_channels * msg_size * sizeof(double))

    // Warmup
    while (receive_count < 500) {
        int size = zmq_recv(socket, message_buffer, sizeof(message_buffer) - 1, 0);
        if (size == -1) {
            fprintf(stderr, "Error receiving message\n");
            break;
        }
        receive_count++;
    }

    receive_count = 0;  // reset count for actual measurement
    double first_timestamp = 0.0;

    while (receive_count < n_messages) {
        int size = zmq_recv(socket, message_buffer, sizeof(message_buffer), 0);
        if (size == -1) {
            fprintf(stderr, "Error receiving message\n");
            break;
        }

        double now = get_monotonic_time();

        // Extract timestamp from message (first double)
        double msg_timestamp;
        memcpy(&msg_timestamp, message_buffer, sizeof(double));

        if (receive_count == 0) {
            first_timestamp = msg_timestamp;
        }

        recv_times[receive_count] = now;
        process_times[receive_count] = now - msg_timestamp;

        receive_count++;
    }

    printf("Total messages processed: %d\n", receive_count);

    // Jitter statistics
    double sum_diff = 0.0;
    double max_diff = 0.0;
    int max_idx = 0;
    double sum_sq_diff = 0.0;

    for (int i = 0; i < n_messages-1; i++) {
        double diff = recv_times[i+1] - recv_times[i];
        recv_times_diff[i] = diff;
        sum_diff += diff;
        sum_sq_diff += diff * diff;
        if (diff > max_diff) {
            max_diff = diff;
            max_idx = i;
        }
    }

    double avg_period_sec = sum_diff / (n_messages - 1);
    double avg_period = avg_period_sec * 1e6;  // us
    double variance = (sum_sq_diff / (n_messages - 1)) - (avg_period_sec * avg_period_sec);  // s²
    double std_period = sqrt(fmax(0.0, variance)) * 1e6;  // us

    printf("Average receive period (us): %.6f\n", avg_period);
    printf("Max receive period (us): %.6f, idx: %d\n", max_diff*1e6, max_idx);
    printf("Std receive period (us): %.6f\n", std_period);

    // Latency statistics
    double sum = 0.0;
    double max = 0.0;
    max_idx = 0;
    double sum_sq = 0.0;

    for (int i=0;i<n_messages;i++) {
        sum += process_times[i];
        sum_sq += process_times[i] * process_times[i];
        if (process_times[i] > max) {
            max = process_times[i];
            max_idx = i;
        }
    }
    double avg_latency_sec = sum / n_messages;
    double avg_latency = avg_latency_sec * 1e6;  // us
    double variance_latency = (sum_sq / n_messages) - (avg_latency_sec * avg_latency_sec);  // s²
    double std_latency = sqrt(fmax(0.0, variance_latency)) * 1e6;  // us

    double throughput = n_messages / (recv_times[n_messages-1] - first_timestamp)/1e3;
    printf("Throughput (msg/ms): %.2f\n", throughput);


    // Save to CSV
    FILE *fp = fopen(output_file, "w");
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", output_file);
        return 1;
    }

    fprintf(fp, "Metric,Recv_period,Latency,Throughput\n");
    fprintf(fp, "mean,%.6f,%.6f,%.6f\n", avg_period, avg_latency, throughput);
    fprintf(fp, "std,%.6f,%.6f, \n", std_period, std_latency);
    fprintf(fp, "max,%.6f,%.6f, \n", max_diff*1e6, max*1e6);

    fclose(fp);

    // Cleanup
    zmq_close(socket);
    zmq_ctx_destroy(context);
    free(recv_times);
    free(process_times);
    free(recv_times_diff);

    printf("Consumer stopped.\n");
    return 0;
}
