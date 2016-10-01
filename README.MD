# mewbase

## The blurb

Mewbase is an engine for persisting, publishing and processing events.

Mewbase unites the concept of a messaging system/event log and a database.

Messaging systems provide transport and distribution of messages from source to target(s). Some messaging
provide durable storage of messages. Others (streaming messaging systems) go further still and allow messages to be
subscribed to and replayed from any point in the past. These kinds of messaging systems can act as an event log.

Many applications now realise that its vital to keep track of all the events produced by your application. (E.g. every
add or remove item event in a shopping basket). These events are a goldmine which can be mined to provide great
business value, so sensible applications store them in a log.

But doing queries over event logs is slow and tricky, as they are not designed with this kind of access in mind,
e.g. "give me the sum of all orders", or to retrieve the *current* state of a shopping basket (event sourcing),
or to provide some kind of report.

Applications typically solve this by maintaining materialized views of the events. E.g. they listen to events, do some
calculation and update some tables, typically in a relational or document database.

This is usually done in an ad-hoc way, cobbling together several pieces of technology, and having to deal with the
synchronization issues between the views and the event log(s).

Wouldn't it be nice if there was some kind of event-db that did all of this for you? Some kind of a hybrid between a
traditional messaging system, and highly scalable, high performance event log and a document/graph database?

This is where mewbase comes in. 

Mewbase will provide a high performance, horizontally scalable, replicated event log, scaling to petabyte size logs
that supports pub/sub functionality. In this way its a bit like an advanced streaming messaging system.

It will also provide an engine to query and update documents, and potentially (going ahead) to maintain links between
 them. In this way it's a bit like a document database.
  
The special sauce that links together the messaging system and database parts are *functions*. Functions are deployed
  by the user and run in mewbase. They listen to events, perform some kind of calculation, and update state in
  documents.
  
The application or service developer just needs to emit events from the application when things happen (e.g. shopping
basket add item/remove item/order) and to query views e.g. to view a shopping basket or get a report. Views aren't
updated by the client so the client has no shared access to mutable state. Win!
  
Some examples of functions might be:
  
* Keeping a current state of all shopping baskets by listening to all add/remove item events.
* Keeping a count/sum/average of all orders.
* Maintaining more complex real-time reports
* Implementing business processes
* Implementing eventually consistent distributed transactions
* Any kind of general purpose stream processing

Mewbase will ensure that functions never lose messages and receive them at least once.

All this functionality will be accessible over a simple wire protocol and via a simple client API which we will
provide in Java, JavaScript and potentially other languages going ahead.

Mewbase engine will also be full embeddable in any Java (or Scala, or Groovy, or...) application so you can
colocate your "server" in your actual service.

Mewbase is a great choice for implementing event driven microservices.

*Mewbase is a new project and very much a work in progress.*

### Features

Mewbase will contain the following features

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
* Fully embeddable
* Standalone option
* Log archiving
* Client helpers for event sourcing (?)

