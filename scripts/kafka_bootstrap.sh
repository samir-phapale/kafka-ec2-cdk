set -euxo pipefail
exec > >(tee -a /var/log/kafka-bootstrap.log) 2>&1

: "${KAFKA_VERSION:?}" "${SCALA_VERSION:?}" "${KAFKA_SHA512:?}" "${TOPIC_NAME:?}" "${TOPIC_PARTITIONS:?}"

BOOTSTRAP_SERVER="127.0.0.1:9092"
KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

dnf install -y aws-cfn-bootstrap java-17-amazon-corretto-headless

useradd --system --no-create-home --shell /sbin/nologin kafka

curl -fsSL -o /tmp/kafka.tgz \
  --connect-timeout 10 --max-time 120 --speed-limit 1048576 --speed-time 30 \
  --retry 3 --retry-delay 5 \
  "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_DIST}.tgz"
echo "${KAFKA_SHA512}  /tmp/kafka.tgz" | sha512sum --check --strict
tar -xzf /tmp/kafka.tgz -C /opt
rm -f /tmp/kafka.tgz
ln -s "/opt/${KAFKA_DIST}" /opt/kafka

mkdir -p /etc/kafka /var/lib/kafka

cat > /etc/kafka/server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://${BOOTSTRAP_SERVER}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/var/lib/kafka
num.partitions=${TOPIC_PARTITIONS}
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF

chown -R kafka:kafka "/opt/${KAFKA_DIST}" /etc/kafka /var/lib/kafka

CLUSTER_ID=$(runuser -u kafka -- /opt/kafka/bin/kafka-storage.sh random-uuid)
runuser -u kafka -- /opt/kafka/bin/kafka-storage.sh format -t "${CLUSTER_ID}" -c /etc/kafka/server.properties

cat > /etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka
After=network-online.target
Wants=network-online.target

[Service]
User=kafka
Group=kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now kafka

for _ in $(seq 60); do
  if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null 2>&1; then
    break
  fi
  sleep 5
done
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "${BOOTSTRAP_SERVER}" > /dev/null

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${TOPIC_NAME}" \
  --partitions "${TOPIC_PARTITIONS}" \
  --replication-factor 1
