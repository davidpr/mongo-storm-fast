# Fast Mongo Storm

This is a fast way to set a up a basic topology that consumes JSON tuples from a Java client.

   * The spout, imported from storm-amqp-spout, is able to cannect to a Rabbitmq queue.
   * The bolt, MongoInsertBolt, uses the Java API to store docuemnts into a MongoDB collection.

---
   *dependencies:*

   * https://github.com/davidpr/storm-amqp-spout
   * https://github.com/davidpr/storm-json
   * https://github.com/davidpr/storm-rabbitmq/tree/master/storm-client-json-regression

---
##Usage

   * Compile and launch the topology
```java
mvn package  && zip -d ./target/mongo-storm-fast-0.0.1-SNAPSHOT.jar defaults.yaml
storm jar ./target/mongo-storm-fast-0.0.1-SNAPSHOT.jar org.mongodb.MongoTopology test
```

   * Launch the client

```java
mvn package
java -cp ./target/storm-client-json-regression-1.0-SNAPSHOT.jar StormSenderJSON stormkey
```

   * Check out the results
```java
tail -f worker6700.log
 ```

 You also can check it out by accessing to your MongoDB collection or visualizing the rabbitmq queue



