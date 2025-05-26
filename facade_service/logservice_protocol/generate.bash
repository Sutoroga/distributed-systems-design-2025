python -m grpc_tools.protoc \
  -Ilogservice_protocol \
  --python_out=logservice_protocol \
  --grpc_python_out=logservice_protocol \
  logservice_protocol/log.proto
