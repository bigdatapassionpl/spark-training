
# Spark Training

Spark 3.5.3 on Java 17+ requires JVM module options to be set
~~~
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
~~~

Before running Pub/Sub Streaming Job, you need to:

Setup:
# Login with default credentials
gcloud auth application-default login

# Create topic and subscription
gcloud pubsub topics create spark-word-count-topic
gcloud pubsub subscriptions create spark-streaming-subscription --topic=spark-word-count-topic

Update configuration in both files:
- gcpProject = "your-gcp-project"

Run:
1. Start the consumer (WordCount) first
2. Start the producer (SimpleProducer) to send messages
