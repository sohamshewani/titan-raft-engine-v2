CXX ?= g++
CC ?= gcc
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -pthread
CFLAGS ?= -O3 -Wall -Wextra

TARGET = titan-raft-engine
TEST_TARGET = titan-raft-tests
BENCH_TARGET = titan-raft-bench

SRC_DIR = src
INCLUDE_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = bench

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS = $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/tests/%.o, $(TEST_SRCS))

BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_OBJS = $(patsubst $(BENCH_DIR)/%.cpp, $(BUILD_DIR)/bench/%.o, $(BENCH_SRCS))

INCLUDES = -I$(INCLUDE_DIR)

.PHONY: all build test clean benchmark directories

all: directories build

directories:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(BUILD_DIR)/tests
	@mkdir -p $(BUILD_DIR)/bench

build: directories $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(OBJS) -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test: directories $(BUILD_DIR)/$(TEST_TARGET)
	@if [ -f $(BUILD_DIR)/$(TEST_TARGET) ]; then \
		./$(BUILD_DIR)/$(TEST_TARGET); \
	else \
		echo "No tests found or test target failed to build."; \
	fi

$(BUILD_DIR)/$(TEST_TARGET): $(OBJS) $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) $(TEST_OBJS) -o $@

$(BUILD_DIR)/tests/%.o: $(TEST_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

benchmark: directories $(BUILD_DIR)/$(BENCH_TARGET)
	@if [ -f $(BUILD_DIR)/$(BENCH_TARGET) ]; then \
		./$(BUILD_DIR)/$(BENCH_TARGET); \
	else \
		echo "No benchmarks found or benchmark target failed to build."; \
	fi

$(BUILD_DIR)/$(BENCH_TARGET): $(OBJS) $(BENCH_OBJS)
	$(CXX) $(CXXFLAGS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) $(BENCH_OBJS) -o $@

$(BUILD_DIR)/bench/%.o: $(BENCH_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

clean:
	@rm -rf $(BUILD_DIR)