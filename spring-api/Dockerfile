FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn clean package -Dmaven.test.skip=true

FROM eclipse-temurin:17-jre-alpine

RUN apk add --no-cache docker-cli curl bash

WORKDIR /app
COPY --from=builder /build/target/spring-api.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]