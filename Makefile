CC = gcc
CFLAGS = -Wall -Wextra -O2 -march=native
LIBS = -lzmq -lm

C_SOURCES := $(shell find . -type f -name '*.c' -not -path './venv/*')
TARGETS := $(patsubst %.c,%.bin,$(C_SOURCES))

.PHONY: all clean

all: $(TARGETS)

%.bin: %.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

clean:
	rm -f $(TARGETS)

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all     - Build all programs (default)"
	@echo "  clean   - Remove built programs"
