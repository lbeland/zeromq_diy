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
    char message_buffer[64 * 1024];

    while (receive_count < n_messages) {
        int size = zmq_recv(socket, message_buffer, sizeof(message_buffer) - 1, 0);
        if (size == -1) {
            fprintf(stderr, "Error receiving message\n");
            break;
        }

        double now = get_monotonic_time();
        recv_times[receive_count] = now;

        // Extract timestamp from message (first double)
        if (size >= sizeof(double)) {
            double msg_timestamp = *((double *)message_buffer);
            process_times[receive_count] = now - msg_timestamp;
        } else {
            process_times[receive_count] = 0.0;
        }

        receive_count++;
    }

    printf("Total messages processed: %d\n", receive_count);

    // Calculate statistics (skip first measurement)
    if (receive_count > 1) {
        double sum_diff = 0.0;
        double max_diff = -1e9;
        int max_idx = 0;
        double sum_sq_diff = 0.0;

        for (int i = 1; i < receive_count; i++) {
            double diff = recv_times[i] - recv_times[i - 1];
            recv_times_diff[i - 1] = diff;
            sum_diff += diff;
            sum_sq_diff += diff * diff;
            if (diff > max_diff) {
                max_diff = diff;
                max_idx = i - 1;
            }
        }

        double avg_period = sum_diff / (receive_count - 1);
        double variance = (sum_sq_diff / (receive_count - 1)) - (avg_period * avg_period);
        double std_period = sqrt(fmax(0.0, variance));

        printf("Average receive period (s): %.6f\n", avg_period);
        printf("Max receive period (s): %.6f, idx: %d\n", max_diff, max_idx);
        printf("Std receive period (s): %.6f\n", std_period);
    }

    // Save to CSV
    FILE *fp = fopen(output_file, "w");
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", output_file);
        return 1;
    }

    fprintf(fp, "recv_time,latency,recv_period\n");
    for (int i = 0; i < receive_count - 1; i++) {
        fprintf(fp, "%.15f,%.15f,%.15f\n", recv_times[i], process_times[i], recv_times_diff[i]);
    }
    // Last row
    fprintf(fp, "%.15f,%.15f,0.0\n", recv_times[receive_count - 1], process_times[receive_count - 1]);

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
