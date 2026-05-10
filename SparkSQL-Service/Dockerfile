# ─────────────────────────────────────────────────────────────────────────────
# IIS-SparkSQL-Service Dockerfile
# Multi-stage build: Maven build → slim runtime image
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /build

# Copy pom first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn clean package -DskipTests -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Spark in local mode writes temp files here
RUN mkdir -p /tmp/spark-warehouse /tmp/spark-events

COPY --from=build /build/target/IIS-SparkSQL-Service-2026.1.jar app.jar

# ── JVM flags ─────────────────────────────────────────────────────────────────
# Required for Spark + Java 21 compatibility:
#   --add-exports / --add-opens: Spark uses internal JDK APIs (sun.nio, java.net)
# Memory: tune via docker-compose environment JAVA_TOOL_OPTIONS
ENV JAVA_TOOL_OPTIONS="\
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -Xms1g -Xmx2g"

# Ports:
#  9990 = Spring Boot REST API
# 10000 = Hive Thrift Server (DBeaver JDBC)
#  4040 = Spark UI (optional debugging)
EXPOSE 9990 10000 4040

ENTRYPOINT ["java", \
  "-Dspark.rest.callback.url=http://iis-spark-sql:9990/IIS-SparkSQL-Service", \
  "-jar", "app.jar"]
