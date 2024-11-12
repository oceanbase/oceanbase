#!/bin/bash
DEVEL_PATH="../../../3rd/usr/local/oceanbase/deps/devel"

CURRENT_BINARY_DIR="./"

PROTOBUF_PROTOC="${DEVEL_PATH}/bin/protoc"
GRPC_CPP_PLUGIN_EXECUTABLE="$DEVEL_PATH/bin/grpc_cpp_plugin"
set -e

if [ ! -f "$PROTOBUF_PROTOC" ]; then
    echo "Error: protoc binary not exists, you should execute \"sh build.sh --init\" first"
    exit 1
fi

PROTO_NAME="newlogstorepb"
proto_path="${CURRENT_BINARY_DIR}${PROTO_NAME}.proto"
echo "generate code for ${proto_path}"
export LD_LIBRARY_PATH=../../../3rd/usr/local/oceanbase/devtools/lib64:$LD_LIBRARY_PATH
$PROTOBUF_PROTOC \
    --grpc_out "generate_mock_code=true:${CURRENT_BINARY_DIR}" \
    --cpp_out "${CURRENT_BINARY_DIR}" \
    --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}" \
        "${proto_path}"

echo "rename .cc files"
proto_ouput_cc="${PROTO_NAME}.pb.cc"
proto_ouput_cpp="${PROTO_NAME}.pb.cpp"
proto_ouput_h="${PROTO_NAME}.pb.h"
proto_grpc_ouput_cc="${PROTO_NAME}.grpc.pb.cc"
proto_grpc_ouput_cpp="${PROTO_NAME}.grpc.pb.cpp"
proto_grpc_ouput_h="${PROTO_NAME}.grpc.pb.h"

mv $proto_ouput_cc $proto_ouput_cpp
mv $proto_grpc_ouput_cc $proto_grpc_ouput_cpp

echo "success generated files:"
echo -e "\t${proto_ouput_h}"
echo -e "\t${proto_ouput_cpp}"
echo -e "\t${proto_grpc_ouput_h}"
echo -e "\t${proto_grpc_ouput_cpp}"
