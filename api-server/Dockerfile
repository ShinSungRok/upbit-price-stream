FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/api-server-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
