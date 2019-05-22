# kafka-spike

Use https://kafka.apache.org/quickstart to start up a zookeeper and some kafka brokers with the following topics created:
- test
- my-replicated-topic (as used in the quick start demo)
- streams-input-1
- streams-input-2
all with String key and value serializers

You can run the KafkaProducer and KafkaConsumer classes to write and read from the my-replicated-topic created in the quickstart demo.

In order to test out Kafka Streams run the KafkaStreamsApp class, and then the KafkaProducerAppForStreams1 and KafkaProducerAppForStreams2 classes to send messages that will be joined and printed in the logs of the KafkaStreamsApp class
