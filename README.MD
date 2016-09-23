# mubase

Mubase is an engine for persisting, publishing and processing events.

It shares some ideas from event logs, document databases and messaging systems and combines them in a unique way.

It handles the plumbing of transport and persistence of events and providing views so you don't have to worry about
dealing with separate messaging systems or databases and can concentrate on the business logic of your service or
application.

You can perhaps think of mubase as an active database for event driving apps and services. Mubase is a great choice
for event driven microservices.

Mubase persists events in append only logs. Clients can subscribe to events which are streamed to them from any point in
 the log. Filters can be used to determine which events to subscribe to. In this sense it can act as an event streaming
 engine.

In mubase all events and documents are BSON objects.

Mubase also maintains documents (BSON objects), much like a document database, allowing you to query them based on their
 fields. However unlike a document database you don't update documents directly - documents are updated automatically
  from functions that you specify which take as input the events.

In this way the documents can be thought of as materialised views of the data. An example would be maintaining a set of
documents that represent current shopping baskets from a sequence of add/remove item events, or maintaining a product
catalogue from add/remove product events.

You can provide functions to run which subscribe to events and do stuff, e.g. update tables or other stuff
(e.g. call services). These can be used for materialised views, or implementing business processes in a reliable way.

Functions can implement distributed eventually consistent transactions.

Mubase is a new project and very much a work in progress.

Mubase will contain the following features

* Multiple event logs scaling to petabyte size
* Horizontal scalability via sharding/partitioning
* Replication
* Event persistence
* Transactional persistence of events
* Transaction updates of multiple documents in same binder 
* Event subscription
* Subscription filters
* User defined functions
* BSON messages
* Persistent documents
* Document queries
* Event streaming
* Document streaming
* Simple wire protocol
* Clients in Java, JS, Go, ...
* Pluggable authentication and authorisation
* TLS for connections with server + client certs
* Metrics
* Embeddable
* Standalone option
* Log archiving
* Client helpers for event sourcing

# Wire protocol

The wire protocol is simple. A sequence of frames are encoded onto the wire.

Each frame is preceded by 4 bytes representing a 32 bit unsigned int in bigendian order. This is the length of the
frame. This is then proceeded by the frame itself which is the binary representation of a BSON object.
 
BSON objects representing frames have two fields, one "type" where the value is the type of the frame, and another
 "frame" which contains the body of the frame. What's in the body depends on the type of the frame, e.g.:
 
    {
        type: "CONNECT"
        frame: {
            version: "1.0"
            username: "tim",
            password: "1234"
        }
    }

## Frames

The protocol should support, initially the following frames:

## RESPONSE

General response frame - sent in response to processing of various other frames

Fields:

* `ok` - mandatory, boolean. `true` for success, `false` for failure
* `errCode` - optional, string. Error code or key in case of failure
* `errMsg` - optional, string. Error message in case of failure

### CONNECT

Sent immediately after creating a TCP connection in order to provide auth information and connection settings.
The client must send a CONNECT before doing any other operations on a connection

Fields:

* `version` - mandatory - the version of the client making the connection.
* `username` - optional - username
* `password` - optional - password
  
The server will respond with a RESPONSE frame. In the case of a failed connect, the server will close the connection
  after sending the RESPONSE frame.
  
Connects can fail for various reasons, including incorrect credentials or unsupported client version.  

### EMIT

Emit an event to the server for storage.

Fields

* `sessID` - optional - int32. unique id of the producer scoped to the connection. Used to group transactional emits
* `eventType` - mandatory - string. Type of the event - must be unique to the stream. E.g. `add_item`
* `event` - mandatory - BSONObject. The event itself.

Events must not be more than X megabytes in size or they will be rejected.

The server will respond with a RESPONSE frame when the event is successfully persisted to permanent storage or if
storage fails. The event will not be distributed to subscribers unless storage succeeds.

### STARTTX

Start a transaction.

Fields

* `sessID` - mandatory - int32. The session id to start a transaction for.

The server will respond with a RESPONSE frame.

Requests to start a Tx will fail if there is already a Tx in progress for the session

### COMMITTX

Commit a transaction.

Fields

* `sessID` - mandatory - int32. The session id to commit a transaction for.

The server will respond with a RESPONSE frame.

Requests to commit a Tx will fail if there is no Tx in progress for the session

### ABORTTX

Abort a transaction.

Fields

* `sessID` - mandatory - int32. The session id to abort a transaction for.

The server will respond with a RESPONSE frame.

Requests to abort a Tx will fail if there is no Tx in progress for the session

### SUBSCRIBE

Subscribe to events from a stream

Fields

* `StreamName` - mandatory - string. The name of the stream to subscribe to, e.g. `com.tesco.basket`
* `EventType` - optional - string. The name of a specific event to subscrive to. If omitted all types of events in the
stream will be subscribed to
* `StartSequence` - optional - int64. The sequence number of events in the stream to start from subscribing from.
* `StartTimestamp` - optional - int64. The earliest timestamp of events in the stream to start from subscribing from.
* `DurableID` - optional - string. Unique id for a durable subscription. If provided then the server will look-up and
resume an existing subscription for that name, otherwise a new durable subscription for that name will be created.
* `Matcher` - optional BSONObject. Object to match on the event fields. Non matching events will be filtered out.
 
if `StartSequence` or `StartTimestamp` are omitted then only events starting from when the subscription was created will
 be received.
 
### SUBRESPONSE

Like a RESPONSE but sent in response to a SUBSCRIBE request - contains an additional fields `subID`

Fields:

* `ok` - mandatory, boolean. `true` for success, `false` for failure
* `errCode` - optional, string. Error code or key in case of failure
* `errMsg` - optional, string. Error message in case of failure
* `subID` - optional, int32. Unique ID of subscription scoped to connection in case of success

### RECEV
 
Event received by a subscription.
 
Fields:
 
* `subID` - mandatory, int32. ID of the client subscription.
* `eventType` - mandatory, string. Type of the event
* `timestamp` - mandatory, int64. Timestamp when the event was persisted.
* `seqNo` - mandatory, int64. Sequence number of the event in the stream.
* `event` - mandatory, BSONObject. The event itself.
 
### ACKEV
 
Sent by client to acknowledge receipt of last event received by a subscription
 
Fields
 
* `subID` - mandatory, int32. ID of the subscription to ack for 
 
### QUERY
 
Sent by client to query documents from a binder

Fields
 
* `queryID` - mandatory, int32. Unique id of query per connection.
* `binder` - mandatory, string. Name of binder to query in.
* `matcher` - mandatory, BSONObject. Matcher to match documents in binder.

The server will respond with a QUERYRESPONSE after processing the query request.

If there are results to return they will be returned as a succession of QUERYRESULT frames on the connection.

The server will allow a maximum of X unacknowledged QUERYRESULT frames to be in transit at any one time. 
 
### QUERYRESPONSE

Sent by server in response to a query

Fields

* `queryID` - mandatory, int32. Unique id of query per connection.
* `numResults` - mandatory, int64. Number of results to return

### QUERYRESULT

Sent by a server holding a single query result

Fields

* `queryID` - mandatory, int32. Unique id of query per connection.
* `result` - mandatory, BSONObject. The query result

### QUERYACK

Sent by client to acknowledge a query result.

Fields

* `queryID` - mandatory, int32. Unique id of query per connection.
 

# Implementation notes

## Event logs

We should use separate event logs for different stream names (?)

We can fork ActiveMQ Artemis log for this. Possibly remove deletion/compaction logic as won't be necessary as
we won't delete events

## Document storage

Again, use Artemis log for this to store the actual document data. Documents can be deleted so retain compaction and
deletion functionality.

Use separate log for each binder.

Use b-tree or hash index on disk for lookup based on primary key to index into log.

For additional indexes use additional hash/b-trees.

## Document caching

LRU cache to cache most recent used docs. Cache binary form of documents in memory for better memory usage.




