FROM java:8-alpine
RUN mkdir /app
WORKDIR /app
COPY target/metrics-scheduler.jar .
CMD java -jar -Xms100m -Xmx300m -Xss512k ./metrics-scheduler.jar
