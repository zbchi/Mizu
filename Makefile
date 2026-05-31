# --------------------------------------
# Proto build config
# --------------------------------------
PROTO_DIR := proto
PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")

LSM_BUILD_DIR := engine/lsmtree/build

PROTOC := protoc
PROTOC_OPTS := -I=$(PROTO_DIR) \
	--go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
	--go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative


all: proto build

lsmtree:
	@cmake -S engine/lsmtree -B $(LSM_BUILD_DIR) -DLSMTREE_BUILD_TESTS=OFF
	@cmake --build $(LSM_BUILD_DIR) -j$$(nproc)

lsmtree-test:
	@cmake -S engine/lsmtree -B $(LSM_BUILD_DIR) -DLSMTREE_BUILD_TESTS=ON
	@cmake --build $(LSM_BUILD_DIR) -j$$(nproc)
	@ctest --test-dir $(LSM_BUILD_DIR) --output-on-failure

build: lsmtree
	@mkdir -p bin
	@go build -o bin/mizu ./cmd/mizu
	@go build -o bin/mizu-client ./cmd/mizu-client

test: lsmtree-test
	@go test ./...

proto:
	@echo "Generating"
	@$(PROTOC) $(PROTOC_OPTS) $(PROTO_FILES)
	@echo "Done"

clean:
	@echo "Cleaning"
	@find $(PROTO_DIR) -name "*.pb.go" -delete
	@echo "Done"

.PHONY: all lsmtree lsmtree-test build test proto clean
