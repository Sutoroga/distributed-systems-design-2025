# Distributed Systems Design 2025

# Micro MQ

.env
```dotenv
CLUSTER_NAME=my-hazelcast-cluster

PUBLIC_IP=<IP>

MC_ADMIN_USER=<USERNAME>
MC_ADMIN_PASSWORD=<PASSWORD>

LOGGING_SERVICE_URL=http://logging-service:8001
MESSAGES_SERVICE_URL=http://messages-service:8002

CONFIG_SERVER_URL=http://config-service:8500

KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_TOPIC=message_queue
KAFKA_GROUP_ID=messages-service-group
```

Check cluster info by spawning shell in `kafka-X` and run:
```bash
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic message_queue --describe
```