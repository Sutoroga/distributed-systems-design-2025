# Distributed Systems Design 2025

# Micro Consul

.env
```dotenv
CLUSTER_NAME=my-hazelcast-cluster
HAZELCAST_NODES=hz-node-1:5701,hz-node-2:5701,hz-node-3:5701

PUBLIC_IP=<IP>

CONSUL_HOST=consul
CONSUL_PORT=8500
CONSUL_HTTP_ADDR: "http://${CONSUL_HOST}:${CONSUL_PORT}"

MC_ADMIN_USER=<USERNAME>
MC_ADMIN_PASSWORD=<PASSWORD>

KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_TOPIC=message_queue
KAFKA_GROUP_ID=messages-service-group
```