FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY build/libs/queueguard-0.0.1-SNAPSHOT.jar app.jar
USER 10001
EXPOSE 18090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
