CC = gcc
CFLAGS = -Wall -Wextra -O2
LIBS = -lzmq -lm

TARGETS = producer.bin consumer.bin

.PHONY: all clean

all: $(TARGETS)

producer.bin: producer.c
	$(CC) $(CFLAGS) -o producer.bin producer.c $(LIBS)

consumer.bin: consumer.c
	$(CC) $(CFLAGS) -o consumer.bin consumer.c $(LIBS)

clean:
	rm -f $(TARGETS)

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all     - Build all programs (default)"
	@echo "  clean   - Remove built programs"
