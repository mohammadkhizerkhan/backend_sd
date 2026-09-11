## kafka

### problem statement

1. simple message queue with multiple partitions is present but if we want to handle event in order, we need to have only one partition which is not scalable so we need to have multiple partitions and we can use partition key to ensure that all events with the same key go to the same partition and are processed in order.
2. If we have have single consumer which is listening to the partitions, then it can become a bottleneck and if it goes down, then we will not be able to process any events. So we need to have multiple consumers belongs to same consumer group which can listen to the partitions and process the events in parallel and distribute the load, kafka will ensure that each event is processed by only one consumer in the consumer group and if any consumer goes down, then the load will be automatically distributed among the remaining consumers in the consumer group.

### components

1. Broker: it is responsible for storing the events in the topic and managing the partitions(queues).
2. partition: it is a physical channel to which the producers send the events and from which the consumers consume the events, it is a part of the topic and it can have multiple partitions to ensure scalability and parallel processing of events.
3. topic: it is a logical grouping of partitions to which the producers send the events and from which the consumers consume the events, it can have multiple partitions to ensure scalability and parallel processing of events.
4. producer: it is responsible for sending the events to the kafka topic, it can specify the partition key to ensure that all events with the same key go to the same partition and are processed in order.
5. consumer: it is responsible for consuming the events from the kafka topic, it can belong to a consumer group which can listen to the partitions and process the events in parallel and distribute the load, kafka will ensure that each event is processed by only one consumer in the consumer group and if any consumer goes down, then the load will be automatically distributed among the remaining consumers in the consumer group.

```
common confustion between topic and partition is that topic is a logical grouping of partitions and partition is a physical channel to which the producers send the events and from which the consumers consume the events, topic can have multiple partitions to ensure scalability and parallel processing of events.
```

```
so basically producer sends the events to the topic, the topic is divided into multiple partitions and each partition can be consumed by only one consumer in the consumer group, if any consumer goes down, then the load will be automatically distributed among the remaining consumers in the consumer group.
```

6. message: it is the actual data that is sent by the producer to the kafka topic and consumed by the consumer from the kafka topic, it can have a key and a value, the key is used to determine the partition to which the message will be sent and the value is the actual data that is sent by the producer to the kafka topic and consumed by the consumer from the kafka topic.

### how does the message flow inside kafka ( with and without key)

1. without key: when the producer sends the message to the kafka topic without specifying the key, then kafka will use round robin strategy to distribute the messages among the partitions, so each message will be sent to a different partition in a round robin manner and each partition can be consumed by only one consumer in the consumer group, if any consumer goes down, then the load will be automatically distributed among the remaining consumers in the consumer group.
2. with key: when the producer sends the message to the kafka topic with specifying the key, then kafka will use the key to determine the partition to which the message will be sent, so all messages with the same key will be sent to the same partition and are processed in order, each partition can be consumed by only one consumer in the consumer group, if any consumer goes down, then the load will be automatically distributed among the remaining consumers in the consumer group.

### scalability

1. reduce the message size: we can reduce the message size by compressing the message before sending it to the kafka topic, this will reduce the network bandwidth and storage requirements and improve the performance of the kafka cluster.
2. more brokers: we can add more brokers to the kafka cluster to increase the storage capacity and processing power of the kafka cluster, this will improve the performance of the kafka cluster and ensure that it can handle more load.
3. chose good partition key: we can choose a good partition key to ensure that all messages with the same key go to the same partition and are processed in order, this will improve the performance of the kafka cluster and ensure that it can handle more load.

### how to fix the hot partition problem

1. use a good partition key: we can use a good partition key to ensure that all messages with the same key go to the same partition and are processed in order, this will improve the performance of the kafka cluster and ensure that it can handle more load. example: if we are sending the events related to a user, then we can use the user id as the partition key to ensure that all events related to the same user go to the same partition and are processed in order.


### fault tolerance and durability
1. configure the acks and replication factor: we can configure the acks and replication factor to ensure that the messages are not lost in case of any failure, acks is the number of acknowledgments that the producer requires from the kafka cluster before considering a message as sent, replication factor is the number of copies of the message that are stored in the kafka cluster, if we set acks to all and replication factor to 3, then the producer will wait for acknowledgments from all the replicas before considering a message as sent and if any replica goes down, then the load will be automatically distributed among the remaining replicas in the kafka cluster.


### errros and retries
1. configure the retries and retry backoff: we can configure the retries and retry backoff to ensure that the messages are not lost in case of any failure, retries is the number of times that the producer will retry sending a message in case of any failure, retry backoff is the time that the producer will wait before retrying to send a message in case of any failure, if we set retries to 3 and retry backoff to 100ms, then the producer will retry sending a message 3 times with a delay of 100ms between each retry in case of any failure.

2. consumer side error handling/retry is not handled by kafka, it is the responsibility of the consumer to handle the errors and retries, if the consumer fails to process a message, then it can either log the error and continue processing the next message or it can retry processing the message after a certain delay, if the consumer fails to process a message after a certain number of retries, then it can either log the error and continue processing the next message or it can send the message to a dead letter queue for further analysis.

### performance tuning
1. configure the batch size and linger ms: we can configure the batch size and linger ms to improve the performance of the kafka cluster, batch size is the number of messages that the producer will send in a single request to the kafka cluster, linger ms is the time that the producer will wait before sending a batch of messages to the kafka cluster, if we set batch size to 100 and linger ms to 10ms, then the producer will wait for 10ms before sending a batch of 100 messages to the kafka cluster.

### advantages

1. user specified distribution strategy using partition key
2. consumer group for load balancing the load coming from multiple producers and fault tolerance, each event is processed by only one consumer in the consumer group
