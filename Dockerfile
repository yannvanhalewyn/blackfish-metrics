FROM java:8-alpine
RUN mkdir /app
WORKDIR /app
COPY target/metrics-scheduler.jar .
CMD java -jar ./metrics-scheduler.jar
