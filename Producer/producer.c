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
#include <errno.h>

double get_monotonic_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

int main(int argc, char *argv[]) {
    int n_messages = 1000;
    int msg_size = 1;
    int num_channels = 1;
    int max_buffer_size = 10;
    char output_file[256] = "send_times.csv";

    // Parse command-line arguments
    int opt;
    static const struct option long_options[] = {
        { "n_messages", required_argument, 0, 'n' },
        { "msg_size", required_argument, 0, 'm' },
        { "num_channels", required_argument, 0, 'c' },
        { "max_buffer_size", required_argument, 0, 'b' },
        { "output_file", required_argument, 0, 'o' },
        0
    };
    while ((opt = getopt_long(argc, argv, "n:m:c:b:o:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'n':
                n_messages = atoi(optarg);
                break;
            case 'm':
                msg_size = atoi(optarg);
                break;
            case 'c':
                num_channels = atoi(optarg);
                break;
            case 'b':
                max_buffer_size = atoi(optarg);
                break;
            case 'o':
                strncpy(output_file, optarg, sizeof(output_file) - 1);
                output_file[sizeof(output_file) - 1] = '\0';
                break;
            default:
                fprintf(stderr, "Usage: %s [options]\n", argv[0]);
                fprintf(stderr, "  -n n_messages (default: 1000)\n");
                fprintf(stderr, "  -m msg_size (default: 1)\n");
                fprintf(stderr, "  -c num_channels (default: 1)\n");
                fprintf(stderr, "  -o output_file (default: send_times.csv)\n");
                return 1;
        }
    }

    int message_size = num_channels * msg_size * sizeof(double);

    // Create ZeroMQ context and socket
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_PUSH);
    zmq_setsockopt(socket, ZMQ_SNDHWM, &(int){max_buffer_size}, sizeof(int));
    int hwm;
    size_t opt_len = sizeof(int);
    zmq_getsockopt(socket, ZMQ_SNDHWM, &hwm, &opt_len);
    printf("Socket HWM: %d\n", hwm);

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

    // Warmup
    for (int i=0; i < 500; i++) {
        double now = get_monotonic_time();
        for (int j = 0; j < num_channels * msg_size; j++) {
            message_data[j] = now;
        }
        zmq_send(socket, message_data, message_size, 0);
    }

    int count = 0;
    for (int i = 0; i < n_messages; i++) {
        double now = get_monotonic_time();
        // Fill message with current timestamp
        for (int j = 0; j < num_channels * msg_size; j++) {
            message_data[j] = now;
        }
        
        zmq_send(socket, message_data, message_size, 0);
        send_times[i] = now;
        count++;
    }

    printf("Total messages sent: %d\n",count);

    // Calculate statistics
    double sum_diff = 0.0;
    double max_diff = 0.0;
    int max_idx = 0;
    double sum_sq_diff = 0.0;

    for (int i = 0; i < n_messages-1; i++) {
        double diff = send_times[i+1] - send_times[i];
        send_times_diff[i] = diff;
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

    printf("Average send period (us): %.6f\n", avg_period);
    printf("Max send period (us): %.6f, idx: %d\n", max_diff * 1e6, max_idx);
    printf("Std send period (us): %.6f\n", std_period);

    // Save to CSV
    char base_output_file[256];
    strncpy(base_output_file, output_file, sizeof(base_output_file) - 1);
    base_output_file[sizeof(base_output_file) - 1] = '\0';

    char metrics_file[300];
    snprintf(metrics_file, sizeof(metrics_file), "%s_Producer.csv", base_output_file);
    FILE *fp = fopen(metrics_file, "w");
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", metrics_file);
        return 1;
    }
    fprintf(fp, "Metric,Send_period\n");
    fprintf(fp, "mean,%.6f\n", avg_period);
    fprintf(fp, "std,%.6f\n", std_period);
    fprintf(fp, "max,%.6f\n", max_diff * 1e6);
    fclose(fp);

    char send_times_file[300];
    snprintf(send_times_file, sizeof(send_times_file), "%s_send_times.csv", base_output_file);
    FILE *fp2 = fopen(send_times_file, "w");
    if (fp2 == NULL) {
        fprintf(stderr, "Error opening file %s\n", send_times_file);
        return 1;
    }
    for (int i = 0; i < n_messages - 1; i++) {
        fprintf(fp2, "%.17f\n", send_times_diff[i]);
    }
    fclose(fp2);

    // Cleanup
    free(send_times);
    free(send_times_diff);
    free(message_data);

    printf("Producer stopped.\n");
    return 0;
}
