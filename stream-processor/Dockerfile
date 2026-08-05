FROM eclipse-temurin:21-jre
WORKDIR /app
COPY stream-processor/build/libs/stream-processor-*.jar app.jar
COPY otel-javaagent.jar otel-javaagent.jar
ENTRYPOINT ["java", "-javaagent:/app/otel-javaagent.jar", "-jar", "app.jar"]
