FROM eclipse-temurin:21-jre
WORKDIR /app
COPY history-api/build/libs/history-api-*.jar app.jar
COPY otel-javaagent.jar otel-javaagent.jar
ENTRYPOINT ["java", "-javaagent:/app/otel-javaagent.jar", "-jar", "app.jar"]
