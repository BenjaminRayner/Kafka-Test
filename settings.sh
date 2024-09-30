unset JAVA_TOOL_OPTIONS
JAVA=java
JAVA_CC=javac

KAFKA_HOME="./kafka_2.12-2.8.2"
export CLASSPATH=.:"${KAFKA_HOME}/libs/*"

STATE_STORE_DIR=/tmp/A4-Kafka-state-store-${USER}

ZKSTRING=localhost:2181
KBROKERS=localhost:9092
STOPIC=student-${USER}
CTOPIC=classroom-${USER}
OTOPIC=output-${USER}
APP_NAME=A4Application-${USER}
