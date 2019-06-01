#!/usr/bin/env bash

SECURITY_PROTOCOL=PLAINTEXT

if [ "$KAFKA_BRIDGE_TLS" = "true" ]; then
    SECURITY_PROTOCOL="SSL"

    if [ -n "$KAFKA_BRIDGE_TRUSTED_CERTS" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
#TLS/SSL
kafka.ssl.truststore.location=/tmp/kafka/bridge.truststore.p12
kafka.ssl.truststore.password=${CERTS_STORE_PASSWORD}
kafka.ssl.truststore.type=PKCS12
EOF
)
    fi

    if [ -n "$KAFKA_BRIDGE_TLS_AUTH_CERT" ] && [ -n "$KAFKA_BRIDGE_TLS_AUTH_KEY" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
kafka.ssl.keystore.location=/tmp/kafka/bridge.keystore.p12
kafka.ssl.keystore.password=${CERTS_STORE_PASSWORD}
kafka.ssl.keystore.type=PKCS12
EOF
)
    fi
fi

if [ -n "$KAFKA_BRIDGE_SASL_USERNAME" ] && [ -n "$KAFKA_BRIDGE_SASL_PASSWORD_FILE" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi

    PASSWORD=$(cat /opt/strimzi/bridge-password/$KAFKA_BRIDGE_SASL_PASSWORD_FILE)

    if [ "x$KAFKA_BRIDGE_SASL_MECHANISM" = "xplain" ]; then
        SASL_MECHANISM="PLAIN"
        JAAS_SECURITY_MODULE="plain.PlainLoginModule"
    elif [ "x$KAFKA_BRIDGE_SASL_MECHANISM" = "xscram-sha-512" ]; then
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_SECURITY_MODULE="scram.ScramLoginModule"
    fi

    SASL_AUTH_CONFIGURATION=$(cat <<EOF
kafka.sasl.mechanism=${SASL_MECHANISM}
kafka.sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE}
 required username="${KAFKA_BRIDGE_SASL_USERNAME}"
  password="${PASSWORD}";
EOF
)
fi

KAFKA_PROPERTIES=$(cat <<-EOF
#Kafka common properties
kafka.bootstrap.servers=${KAFKA_BRIDGE_BOOTSTRAP_SERVERS}
kafka.security.protocol=${SECURITY_PROTOCOL}
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}
EOF
)

PRODUCER_PROPERTIES="#Apache Kafka Producer"

for i in $KAFKA_BRIDGE_PRODUCER_CONFIG; do

        key="kafka.producer.$(echo $i | cut -d'=' -f1)"
        value="$(echo -n $i | cut -d'=' -f2)"
        PRODUCER_PROPERTIES=$(cat <<EOF
$PRODUCER_PROPERTIES
${key}=${value}
EOF
)
done


CONSUMER_PROPERTIES="#Apache Kafka Consumer"
for i in $KAFKA_BRIDGE_CONSUMER_CONFIG; do
        key="kafka.consumer.$(echo $i | cut -d'=' -f1)"
        value="$(echo -n $i | cut -d'=' -f2)"
        CONSUMER_PROPERTIES=$(cat <<EOF
$CONSUMER_PROPERTIES
${key}=${value}
EOF
)
done

HTTP_PROPERTIES=$(cat <<-EOF
#HTTP configuration
http.enabled=${KAFKA_BRIDGE_HTTP_ENABLED}
http.host=${KAFKA_BRIDGE_HTTP_HOST}
http.port=${KAFKA_BRIDGE_HTTP_PORT}
EOF
)
AMQP_PROPERTIES=$(cat <<-EOF
#AMQP configuration
amqp.enabled=${KAFKA_BRIDGE_AMQP_ENABLED}
amqp.host=${KAFKA_BRIDGE_AMQP_HOST}
amqp.port=${KAFKA_BRIDGE_AMQP_PORT}
amqp.mode=${KAFKA_BRIDGE_AMQP_MODE}
amqp.flowCredit=${KAFKA_BRIDGE_AMQP_FLOW_CREDIT}
amqp.certDir=${KAFKA_BRIDGE_AMQP_CERT_DIR}
amqp.messageConverter=${KAFKA_BRIDGE_AMQP_MESSAGE_CONVERTER}
EOF
)

# if http/amqp is disabled, do not print its configuration
PROPERTIES=$(cat <<EOF
$KAFKA_PROPERTIES

$PRODUCER_PROPERTIES

$CONSUMER_PROPERTIES
EOF
)
if [[ -n "$KAFKA_BRIDGE_HTTP_ENABLED" && "$KAFKA_BRIDGE_HTTP_ENABLED" = "true" ]]; then
	PROPERTIES=$(cat <<EOF
$PROPERTIES

$HTTP_PROPERTIES
EOF
)
fi

if [[ -n "$KAFKA_BRIDGE_AMQP_ENABLED" && "$KAFKA_BRIDGE_AMQP_ENABLED" = "true" ]]; then
	PROPERTIES=$(cat <<EOF
$PROPERTIES

$AMQP_PROPERTIES
EOF
)
fi

cat <<EOF
$PROPERTIES
EOF