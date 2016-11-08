# mewbase

## The problem

Smart organisations now realise that its vital to keep track of all the streams of events they produce.

Exammples of streams of events might be:

* Shopping basket events (add/remove/order) from a website
* Events from IoT sensors
* Stock movement events in a supply chain

These events often need to be persisted fast and in high volume and potentially stored for very long periods of time. 

Storing logs of events provides many advantages including

* Complete audit trail of everything that happened
* Events can be replayed to for back-testing or debugging
* Analytics can be run on the events to extract business value
* Machine learning models can be trained in the events to do all sorts of funky stuff
* Events can be used to co-ordinated busines processes between different services
* Functions can be used to implement distributed eventually consistent transactions

But many applications typically want to deal with *aggregated* event data, not the raw event data. Examples would
include:

* A website wants to see the *current* state of a shopping basket, not deal with the raw add/remove item events
* A report needs to display average temperature from IoT sensors broken down by region in real-time
* A stock inventory needs to maintain current stock of products per warehouse
* A business process/saga needs to listen to events and maintain state

This leads many applications either to either:

* Not capture raw events at all - only deal with the current state of their objects - thus losing valuable business
data
* Maintain their own "views" on the event log data perhaps by subscribing to messages and updating tables or documennts
in a separate database

The latter is usually implemented in an ad-hoc way for each application and requires several moving parts, and has to deal
with the synchronization between events and views. What's more it means the client has to deal with accessing two separate
pieces of technology - a messaging system and a database.

## Introducing mewbase

Mewbase provides an engine that functions as a high performance streaming event store but also
 allows you to run persistent *functions* in the engine that listen to raw events and maintain multiple *views* of
 the event data.
 
The views can then be queried in similar way to how you would with a document or graph database.

So you can think of mewbase as combining the following:

* An event streaming engine
* A document/graph database
* Functions which map events to documents

An interesting observation is, unlike a normal document database, the documents are not updated directly by clients,
they are only updated in the engine by the functions.

The client simply sends events (*add item to basket*), and issues queries (*give me current state of basket 1234*).
That means the client has no access to shared mutable state - a big win!  

The events, functions and documents are all maintained in the same engine, mewbase keeps track of synchronization
between the events and the views so you know they are always up to date, irrespective of whether any client
applications or services are connected.

Since the functions run on the server so you can write them in your normal programming language - no need to learn
another language like SQL. The same applies to queries.

All this functionality will be accessible over a simple wire protocol and via a simple client API which we will
provide in Java, JavaScript and potentially other languages going ahead.

Mewbase engine will also be full embeddable in any Java (or Scala, or Groovy) application so you can
colocate your "server" in your actual service, or if you prefer a standalone server or cluster of servers mewbase will
support that setup too.

Mewbase is a great match for implementing event driven microservices using an event sourcing / CQRS pattern.

Mewbase will be designed from scratch to cope with very large datasets and optimised for modern storage.

### Features

Some of the features / future features:

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
* Completely non blocking, and reactive APIS
* Live persistent queries - sends more matching data to client as it arrives on server 
* Sidecars for node.js/go

### What does this look like from a client perspective?

Let's take a simple example of an order microservice whose job it is to maintain shopping baskets for customers.
It has a REST API and is designed using CQRS/event sourcing principles. It implements the following commands:

* Add item to basket
* Remove item from basket
* Place order

and provide the following queries:

* View shoping basket

As commands come in the service processes them and emits events by sending them to the mewbase engine. When the service
wants to get current state of a shopping basket it simply asks mewbase for it. The service provides a function that
maps the events to the basket which runs in the engine.

Each service can have its own engine - this plays nicely with the microservice architectural style where 
services maintain their own data. Or alternatively different services can share a standalone engine or a cluster,
depending on which architectural style you prefer.

The mewbase client will provide caching at the client side and live results which update automatically so you don't
need to reissue the same queries every time you want to get the information.

Mewbase works really well with event driven microservices but is not limited to applications that use that
architectural style - it's applicable to any application that wants high performance event streaming and/or
document style views on the events.

### Why the name "mewbase"?

"mew" is the UK pronunciation of the Greek letter μ. This letter is often used in the scientific world to represent
"micro" i.e. 10^-6 or more loosely "something small". Mewbase is a great choice for event based microservices hence
the connection.

Also, more importantly, "mew" is the sound made by a kitten, and it would be great to have a logo of a cute kitten for
the project. (Any volunteers?)

## Get involved!

This project has a lot of scope and there's a lot to do with some serious engineering challenges.

If you'd like to get involved and be a part of it, please say hi in our
[google group](https://groups.google.com/forum/#!forum/mewbase).