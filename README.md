# Fast Mongo Storm

This is a fast way to set a up a basic topology that consumes JSON tuples from a Java client.


*The spout, imported from the dependency storm-amqp-spout, is able to cannect to a Rabbitmq queue
*The bolt, MongoInsertBolt, uses the Java API to store docuemnts into a MongoDB collection.

---
##dependencies
    * https://github.com/davidpr/storm-amqp-spout
    * https://github.com/davidpr/storm-json

---
##Usage

*Launch the toplogy
```java

```

*Launch the client


*Check out the results




