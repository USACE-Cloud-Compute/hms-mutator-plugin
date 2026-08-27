# Makefile for Go application

# Variables
BINARY_NAME = hms-mutator
COMMIT = $(shell git describe --tags --always --long)
DATE = $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

# Build flags
LDFLAGS = -ldflags "-X main.commit=$(COMMIT) -X main.date=$(DATE)"

# Default target
all: build

# Build the application
build:
	go build $(LDFLAGS) -o $(BINARY_NAME)

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -f $(BINARY_NAME)

# Run the application
run: build
	./$(BINARY_NAME)

.PHONY: all build test clean run