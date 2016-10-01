# Wire protocol

The wire protocol is simple. A sequence of BSON objects are encoded onto the wire.

BSON objects representing holding have two fields, one "type" where the value is the type of the frame, and another
 "frame" which contains the body of the frame. What's in the body depends on the type of the frame, e.g.:
 
    {
        type: "CONNECT"
        frame: {
            version: "1.0"
            username: "tim",
            password: "1234"
        }
    }
    
In BSON, the first 4 bytes contain the length of the rest of the encoded form, making them easy to parse.    

## Frames

The protocol should support, initially, the following frames:

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

## RESPONSE

Generic response

Fields:

* `ok` - mandatory, boolean. `true` for success, `false` for failure
* `errCode` - optional, string. Error code or key in case of failure
* `errMsg` - optional, string. Error message in case of failure


### EMIT

Emit an event to the server for storage.

Fields

* `streamName` - mandatory - string. The event stream to emit to.
* `eventType` - mandatory - string. Type of the event - must be unique to the stream. E.g. `add_item`
* `event` - mandatory - BSONObject. The event itself.
* `sessID` - optional - int32. unique id of the producer scoped to the connection. Used to group transactional emits

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

* `streamName` - mandatory - string. The name of the stream to subscribe to, e.g. `com.tesco.basket`
* `eventType` - optional - string. The name of a specific event to subscrive to. If omitted all types of events in the
stream will be subscribed to
* `startSeq` - optional - int64. The sequence number of events in the stream to start from subscribing from.
* `startTimestamp` - optional - int64. The earliest timestamp of events in the stream to start from subscribing from.
* `durableID` - optional - string. Unique id for a durable subscription. If provided then the server will look-up and
resume an existing subscription for that name, otherwise a new durable subscription for that name will be created.
* `matcher` - optional BSONObject. Object to match on the event fields. Non matching events will be filtered out.
 
if `startSeq` or `startTimestamp` are omitted then only events starting from when the subscription was created will
 be received.
 
### SUBRESPONSE

Like a RESPONSE but sent in response to a SUBSCRIBE request - contains an additional fields `subID`

Fields:

* `ok` - mandatory, boolean. `true` for success, `false` for failure
* `errCode` - optional, string. Error code or key in case of failure
* `errMsg` - optional, string. Error message in case of failure
* `subID` - optional, int32. Unique ID of subscription scoped to connection in case of success

### UNSUBSCRIBE

Unsubscribe a subscription

Fields

* `subID` - mandatory - int32. The id of the subscription to unsubscribe

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