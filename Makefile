# --------------------------------------
# Proto build config
# --------------------------------------
PROTO_DIR := proto
PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")

PROTOC := protoc
PROTOC_OPTS := -I=$(PROTO_DIR) \
	--go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
	--go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative


all: proto

proto:
	@echo "Generating"
	@$(PROTOC) $(PROTOC_OPTS) $(PROTO_FILES)
	@echo "Done"

clean:
	@echo "Cleaning"
	@find $(PROTO_DIR) -name "*.pb.go" -delete
	@echo "Done"

.PHONY: all proto clean
