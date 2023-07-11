PROJECT=timescaledb-backfill
RUST_LOG?=$(PROJECT)=trace
RUST_BACKTRACE?=1
RUST_TEST_THREADS?=4

# The docker registry where we push the images
REGISTRY?=localhost:5000
# pgstats repository
REPOSITORY?=timescale/$(PROJECT)
# pgstats image tag
TAG?=latest
PROJECT_IMAGE=$(REGISTRY)/$(REPOSITORY):$(TAG)
# use the default image the makefile builds for the
# test image target, but allow overriding it.
DOCKER_IMAGE?=$(PROJECT_IMAGE)

# Default this to amd64 for building docker containers on arm machines, but
# allow us to specify it if we explicitly want to test builds on arm as well.
DOCKER_PLATFORM?=linux/amd64

CLIPPY_TARGET?=debug
CLIPPY_PROFILE?=dev

all: help

.PHONY: run
run: # cargo run
	cargo run --release --

release: CLIPPY_PROFILE=release
.PHONY: release
release: build # cargo build --release

.PHONY: build
build: # cargo build
	cargo build --profile $(CLIPPY_PROFILE)

.PHONY: clean
clean: # cargo clean
	cargo clean

.PHONY: test
test: # cargo test
	cargo test -- --test-threads=$(RUST_TEST_THREADS)

.PHONY: test-image
test-image: # run the integration suite against the specified DOCKER_IMAGE. DOCKER_IMAGE?=$(PROJECT_IMAGE) by default.
	USE_DOCKER=true DOCKER_IMAGE=$(DOCKER_IMAGE) DOCKER_PLATFORM=$(DOCKER_PLATFORM) cargo test --test '*' -- --test-threads=$(RUST_TEST_THREADS)

.PHONY: short-test
short-test: # cargo test, but run only the unit tests
	cargo test --bins && cargo test -p test-common

.PHONY: fmt
fmt: # cargo fmt
	cargo fmt

.PHONY: clippy
clippy: # cargo clippy
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: lint
lint: fmt clippy # cargo fmt and clippy

.PHONY: build-image
build-image: # build a docker release image
	docker build \
		--platform $(DOCKER_PLATFORM) \
		-t $(PROJECT_IMAGE) \
		-f Dockerfile \
		--target release .

HELP_TARGET_DEPTH ?= \#
help: # Show how to get started & what targets are available
	@printf "This is a list of all the make targets that you can run, e.g. $(BOLD)make test$(NORMAL)\n\n"
	@awk -F':+ |$(HELP_TARGET_DEPTH)' '/^[0-9a-zA-Z._%-]+:+.+$(HELP_TARGET_DEPTH).+$$/ { printf "$(GREEN)%-20s\033[0m %s\n", $$1, $$3 }' $(MAKEFILE_LIST)
	@echo
