FROM eclipse-temurin:21-jdk AS build

WORKDIR /app

COPY .mvn .mvn
COPY mvnw mvnw
COPY pom.xml pom.xml
COPY config config

COPY taskflow-spi/pom.xml taskflow-spi/pom.xml
COPY taskflow-core/pom.xml taskflow-core/pom.xml
COPY plugins/conversion/pom.xml plugins/conversion/pom.xml
COPY plugins/text/pom.xml plugins/text/pom.xml
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
COPY --from=build /app/taskflow-coordinator/target/taskflow-coordinator-1.0-SNAPSHOT-jar-with-dependencies.jar taskflow-coordinator/target/taskflow-coordinator-1.0-SNAPSHOT-jar-with-dependencies.jar
COPY --from=build /app/taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-jar-with-dependencies.jar taskflow-peer/target/taskflow-peer-1.0-SNAPSHOT-jar-with-dependencies.jar

RUN mkdir -p target/demo-input target/demo-results target/rabbitmq-results

ENV TASKFLOW_TRANSPORT=rabbitmq
ENV TASKFLOW_RABBITMQ_HOST=rabbitmq
ENV TASKFLOW_RABBITMQ_PORT=5672
ENV TASKFLOW_RABBITMQ_USERNAME=guest
ENV TASKFLOW_RABBITMQ_PASSWORD=guest
ENV TASKFLOW_RABBITMQ_DURABLE=false
ENV TASKFLOW_RABBITMQ_EXCHANGE=taskflow.compose.exchange
ENV TASKFLOW_RABBITMQ_QUEUE_PREFIX=taskflow.compose
ENV TASKFLOW_RABBITMQ_PREFETCH=3
