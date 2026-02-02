
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

Run:
1. Start the consumer (WordCount) first
2. Start the producer (SimpleProducer) to send messages


Setup:
gcloud pubsub topics create product-avro-topic
gcloud pubsub subscriptions create product-avro-subscription --topic=product-avro-topic

Run:
1. Start PubSubProductAvroReader first
2. Start PubSubProductAvroProducer to send messages


To use GCP's Schema Registry, you need to:

1. Create schema in GCP:
   gcloud pubsub schemas create product-avro-schema \
   --type=avro \
   --definition='{
       "type": "record",
       "name": "ProductMessageAvro",
       "namespace": "com.bigdatapassion.pubsub.dto",
       "fields": [
           {"name": "creationDate", "type": ["null", "string"]},
           {"name": "id", "type": ["null", "long"]},
           {
               "name": "product",
               "type": ["null", {
                   "type": "record",
                   "name": "ProductAvro",
                   "fields": [
                       {"name": "productName", "type": ["null", "string"]},
                       {"name": "color", "type": ["null", "string"]},
                       {"name": "material", "type": ["null", "string"]},
                       {"name": "price", "type": ["null", "string"]},
                       {"name": "promotionCode", "type": ["null", "string"]}
                   ]
               }]
           }
       ]
   }'

2. Create topic with schema:
   gcloud pubsub topics create product-avro-topic \
   --schema=product-avro-schema \
   --message-encoding=binary

3. Create subscription:
   gcloud pubsub subscriptions create product-avro-subscription \
   --topic=product-avro-topic