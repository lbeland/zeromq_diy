/*
 * Producer node that sends messages via ZeroMQ.
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
    double freq = 1000.0;
    int msg_size = 1;
    int num_channels = 1;
    char output_file[256] = "send_times.csv";

    // Parse command-line arguments
    int opt;
    static const struct option long_options[] = {
        { "n_messages", required_argument, 0, 'n' },
        { "freq", required_argument, 0, 'f' },
        { "msg_size", required_argument, 0, 'm' },
        { "num_channels", required_argument, 0, 'c' },
        { "output_file", required_argument, 0, 'o' },
        0
    };
    while ((opt = getopt_long(argc, argv, "n:f:m:c:o:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'n':
                n_messages = atoi(optarg);
                break;
            case 'f':
                freq = atof(optarg);
                break;
            case 'm':
                msg_size = atoi(optarg);
                break;
            case 'c':
                num_channels = atoi(optarg);
                break;
            case 'o':
                strncpy(output_file, optarg, sizeof(output_file) - 1);
                output_file[sizeof(output_file) - 1] = '\0';
                break;
            default:
                fprintf(stderr, "Usage: %s [options]\n", argv[0]);
                fprintf(stderr, "  -n n_messages (default: 1000)\n");
                fprintf(stderr, "  -f freq in Hz (default: 1000)\n");
                fprintf(stderr, "  -m msg_size (default: 1)\n");
                fprintf(stderr, "  -c num_channels (default: 1)\n");
                fprintf(stderr, "  -o output_file (default: send_times.csv)\n");
                return 1;
        }
    }

    double period = 1.0 / freq;
    int message_size = num_channels * msg_size * sizeof(double);

    // Create ZeroMQ context and socket
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PUSH);
    zmq_bind(socket, "ipc:///tmp/zeromq_comm.sock");

    printf("Producer starting... Sending messages via IPC socket\n");

    // Allocate arrays for tracking times
    double *send_times = (double *)malloc(n_messages * sizeof(double));
    double *send_times_diff = (double *)malloc((n_messages - 1) * sizeof(double));
    double *message_data = (double *)malloc(message_size);

    if (!send_times || !send_times_diff || !message_data) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }

    // Fill with default data (ones)
    for (int i = 0; i < num_channels * msg_size; i++) {
        message_data[i] = 1.0;
    }

    int send_count = 0;
    double next_deadline = get_monotonic_time();

    for (int i = 0; i < n_messages; i++) {
        while (1) {
            double now = get_monotonic_time();
            if (now >= next_deadline) {
                // Fill message with current timestamp
                for (int j = 0; j < num_channels * msg_size; j++) {
                    message_data[j] = now;
                }
                zmq_send(socket, message_data, message_size, 0);
                send_times[send_count] = now;
                send_count++;
                next_deadline += period;
                break;
            }
        }
    }

    printf("Total messages sent: %d\n", send_count);

    // Calculate statistics (skip first measurement)
    double sum_diff = 0.0;
    double max_diff = -1e9;
    int max_idx = 0;
    double sum_sq_diff = 0.0;

    for (int i = 1; i < send_count; i++) {
        double diff = send_times[i] - send_times[i - 1];
        send_times_diff[i - 1] = diff;
        sum_diff += diff;
        sum_sq_diff += diff * diff;
        if (diff > max_diff) {
            max_diff = diff;
            max_idx = i - 1;
        }
    }

    double avg_period = sum_diff / (send_count - 1);
    double variance = (sum_sq_diff / (send_count - 1)) - (avg_period * avg_period);
    double std_period = sqrt(fmax(0.0, variance));

    printf("Average send period (s): %.6f\n", avg_period);
    printf("Max send period (s): %.6f, idx: %d\n", max_diff, max_idx);
    printf("Std send period (s): %.6f\n", std_period);

    // Save to CSV
    FILE *fp = fopen(output_file, "w");
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", output_file);
        return 1;
    }

    fprintf(fp, "send_time,send_period\n");
    for (int i = 0; i < send_count - 1; i++) {
        fprintf(fp, "%.15f,%.15f\n", send_times[i], send_times_diff[i]);
    }
    // Last row
    fprintf(fp, "%.15f,0.0\n", send_times[send_count - 1]);

    fclose(fp);

    // Cleanup
    zmq_close(socket);
    zmq_ctx_destroy(context);
    free(send_times);
    free(send_times_diff);
    free(message_data);

    printf("Producer stopped.\n");
    return 0;
}
