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


