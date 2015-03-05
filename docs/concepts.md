#**Concepts**
Etherd is a platform for continuous stream processing on a cloud scale. The goal is to keep the design functional
and extensible by building on top of a few basic elements. 

##**Model**
###**Events**
An Event is business specific immutable data point in time and is composed of a [Key, Tuple] pair. Related events are grouped into a topic and within a topic events have a global ordering which once defined cannot be changed.

###**Streams**
Streams model continuous flow of events and can either be readable or writable. Readable event streams can be subscribed to for events and act as a primary interface for processsing them. A lot of complexities of managing streams and events can be hidden from the end user by a providing a simple interface like this. 
Type of streams:
**Source streams**: Streams that will ingest events from external sources eg: twitter streams, click streams, databases etc. 
**Intermediate Streams:** Streams that are created for events to flow between stages of a topology. 
**Sink Streams:** Writable streams that will persist the events to a datastore or send the events to some remote destination.

###**Stream Processing Nodes (SPN)**
SPN's define processing and/or transformation logic on events that flow through streams. A single SPN can have multiple input streams to which it subscribes to for events, processes the events and sends a new stream out through 1 or more output streams. Overall the processing can be modeled as a directed acyclic graph where edges are streams and nodes are SPN's.

###**Message Bus**
A message bus acts as a central buffer that provides isolation between consumers, fault tolerance, state persistence, data partitioning and persistence for intermediate streams. All processing happens within the context for a message bus.

##**Topic**
A topic is a partitioned stream of related events eg: *flight.AL156.windspeed*.

###**Topology**
A topology is specification of a full computation graph that can be executed on a distributed cluster and is long running. It will usually define an algorithm or a basic computation. eg: clustering or pattern matching. Multiple topologies can be reused and composed together to define high level business use cases (fraud detection, targeted marketing or disaster predictions)

##**Distributed Execution**
Etherd is a distributed stream execution platform. Once a topology computation is defined, Etherd will optimize the computation, break the computation into parallel stages and run the stages on a distributed cluster via a resource management framework like Mesos or Yarn. Stages model an ordered exection of the topology. Examples:

``` scala
val aggregation = (key, tuple, aggSoFar, count) => ((tuple._2 + aggSoFar)/count) //define an aggregation function
val tp = Topology("product_click_aggregations") //create a new topology
tp.ingest(ClickStream("electronics").aggregate(aggregation, window = 1 hour).sinkTsdb() // define the computation graph
tp.run() //execute it
```
Here we are continuously aggregating a click stream for a specific product category over 1 hour window and then pushing those to some time series database. This is a very simple topology that consists of a single stage executing on a distributed cluster. It will look something like this:

Another example:
``` scala
val tp = Topology("a_simple_join_and_sink", partitions=4)
tp.ingest(ClickStream()).groupBy((key, tuple)=>tuple._3, window = 10 minutes).sinkHdfs()
tp.run()
```
Here is another example where we are grouping an event stream based on some value in the tuple and pushing those to hdfs. This topology might look something like this. We are also specifying the partitions that we want for load balancing. Partitions are executed in their own resource containers 

