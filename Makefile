PROTO_SRC_DIR := proto/proto
PROTO_OUT_DIR := proto/pkg

PROTO_FILES := $(shell find $(PROTO_SRC_DIR) -name "*.proto")

PROTOC := protoc
PROTOC_OPTS := -I=$(PROTO_SRC_DIR) \
	--go_out=$(PROTO_OUT_DIR) --go_opt=paths=source_relative \
	--go-grpc_out=$(PROTO_OUT_DIR) --go-grpc_opt=paths=source_relative

all: proto

proto:
	@echo " Generating protobuf Go files..."
	@$(PROTOC) $(PROTOC_OPTS) $(PROTO_FILES)
	@echo " Done. Generated files are in $(PROTO_OUT_DIR)"

clean:
	@echo " Cleaning generated files..."
	@find $(PROTO_OUT_DIR) -name "*.pb.go" -delete
	@echo " Clean done."

.PHONY: proto clean