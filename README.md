# The Mongo Storm library

The Mongo Storm library let's you leverage MongoDB in your Storm toplogies. It includes two main components.

* A spout for the MongoDB oplog and any MongoDB capped collections, that lets you filter down documents to specific touples in case you want to lower the amount of data transmitted from your spout. Alternatively you can emit the entire document. It's threaded and non-blocking.
* A insert/update bolt that lets you map a tuple either to  a new document or to an existing document in your MongoDB instance.

## Capped Collection Spout Examples

```java
MongoCappedCollectionSpout(String url, String collectionName, DBObject query, MongoObjectGrabber mapper)
```
The options are
	
	* url : the Mongodb connection url
	* collectionName : the name of the capped colletion we wish to spout from
	* query[optional] : a filtering query if we wish to only extract specific documents
	* mapper[optional] : an optional mapper instance that maps the object to a tuple list

### A simple capped collection Spout an object mapped to a custom tuple
This simple example extracts the number of users from document in a capped collection as they arrive come in. Boilercode omited for brevities sake.






