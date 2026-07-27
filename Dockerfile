FROM eclipse-temurin:21-jdk AS build

WORKDIR /app

COPY .mvn .mvn
COPY mvnw mvnw
COPY pom.xml pom.xml
COPY config config

COPY taskflow-spi/pom.xml taskflow-spi/pom.xml
COPY taskflow-core/pom.xml taskflow-core/pom.xml
COPY taskflow-persistence-sqlite/pom.xml taskflow-persistence-sqlite/pom.xml
COPY taskflow-objectstore-minio/pom.xml taskflow-objectstore-minio/pom.xml
COPY plugins/example/pom.xml plugins/example/pom.xml
COPY plugins/example/model/pom.xml plugins/example/model/pom.xml
COPY plugins/example/server/pom.xml plugins/example/server/pom.xml
COPY plugins/example/client/pom.xml plugins/example/client/pom.xml
COPY plugins/example/peer/pom.xml plugins/example/peer/pom.xml
COPY plugins/example/harness/pom.xml plugins/example/harness/pom.xml
COPY plugins/conversion/pom.xml plugins/conversion/pom.xml
COPY plugins/conversion/model/pom.xml plugins/conversion/model/pom.xml
COPY plugins/conversion/server/pom.xml plugins/conversion/server/pom.xml
COPY plugins/conversion/client/pom.xml plugins/conversion/client/pom.xml
COPY plugins/conversion/peer/pom.xml plugins/conversion/peer/pom.xml
COPY plugins/text/pom.xml plugins/text/pom.xml
COPY plugins/text/model/pom.xml plugins/text/model/pom.xml
COPY plugins/text/server/pom.xml plugins/text/server/pom.xml
COPY plugins/text/client/pom.xml plugins/text/client/pom.xml
COPY plugins/text/peer/pom.xml plugins/text/peer/pom.xml
COPY taskflow-transport-rabbitmq/pom.xml taskflow-transport-rabbitmq/pom.xml
COPY taskflow-coordinator/pom.xml taskflow-coordinator/pom.xml
COPY taskflow-peer/pom.xml taskflow-peer/pom.xml
COPY taskflow-gui/pom.xml taskflow-gui/pom.xml
RUN chmod +x mvnw && ./mvnw -B -pl taskflow-coordinator,taskflow-peer -am -DskipTests dependency:go-offline

COPY . .

RUN ./mvnw -B -pl taskflow-coordinator,taskflow-peer -am -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /app/config config
COPY --from=build /app/taskflow-coordinator/target/taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar taskflow-coordinator/target/taskflow-coordinator-1.0-SNAPSHOT-coordinator-runtime.jar
COPY --from=build /app/taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-combined-runtime.jar taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-combined-runtime.jar

RUN mkdir -p target/demo-input target/demo-results target/rabbitmq-results

ENV TASKFLOW_RABBITMQ_HOST=rabbitmq
ENV TASKFLOW_RABBITMQ_PORT=5672
ENV TASKFLOW_RABBITMQ_USERNAME=guest
ENV TASKFLOW_RABBITMQ_PASSWORD=guest
ENV TASKFLOW_RABBITMQ_DURABLE=false
ENV TASKFLOW_RABBITMQ_EXCHANGE=taskflow.compose.exchange
ENV TASKFLOW_RABBITMQ_QUEUE_PREFIX=taskflow.compose
ENV TASKFLOW_RABBITMQ_PREFETCH=3
