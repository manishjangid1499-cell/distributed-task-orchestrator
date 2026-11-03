# Production Runtime Image for Nexus Distributed Task Orchestrator
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S nexus && adduser -S nexus -G nexus

COPY target/*.jar app.jar

RUN chown -R nexus:nexus /app
USER nexus

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-jar", "app.jar"]
