# Updated Makefile to reflect new project structure

SRC_DIR = src
TEST_DIR = tests
BUILD_DIR = build
INCLUDE_DIR = include

SRC_FILES = $(wildcard $(SRC_DIR)/*.c)
TEST_FILES = $(wildcard $(TEST_DIR)/*.c)
OBJ_FILES = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRC_FILES))

CC = gcc
CFLAGS = -I$(INCLUDE_DIR) -Wall -g
LDFLAGS =

all: $(BUILD_DIR)/main

$(BUILD_DIR)/main: $(OBJ_FILES)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR)

test: all
	for test_file in $(TEST_FILES); do \
		./$$test_file; \
	done

.PHONY: all clean test
