carbonsearch: graphite-shaped search queries
--------------------------------------------

CarbonSearch is a search service for graphite that allows for querying metrics
by attributes _other_ than the metric name.

It takes a fake 'metric' like:

    virt.v1.lb-pool:www.discovery-live:true.server-state:installed

and resolves it to a set of metrics, which were previously tagged to match these
characteristics (right now using AND semantics).


Query language
--------------
The query language is just a set of AND'd key-value pairs. Each key-value pair
has a prefix that indicates the data source, like `lb-pool` for load balancer pool, or `discovery-live`
for service discovery liveness. The token as a whole (`lb-pool:www`) is called a __tag__.

Special data sources
--------------------
There's a fake data source called 'text-filter' which can be used as a final filter on
metric name if the KV queries are returning too many things:

    virt.v1.text-filter:Delay.lb-pool:db

This will take all metrics tagged with `lb-pool:db` and filter for ones that
contain `Delay` in the name, so you might end up with a bunch of metrics about replication
delay in the db pool.

Configuration and Running
-------------------------
See `*.example.yaml` for complete example configs with comments. Just `cp` to `$config_name.yaml` to use for real.

`config.yaml`
-------------
Requires `-config` to specify a `config.yaml`, defaults to in the running
directory.  The `consumers` key specifies which consumers should be started by
carbonsearch.

For example,

    consumers:
        httpapi: "httpapi.yaml"

Would only start the HTTP API consumer, not the Kafka one.

Where it runs
-------------
This is an in-memory service intended to run on [CarbonZipper](https://github.com/dgryski/carbonzipper) hosts. consuming from
a handful of Kafka topics to populate the index. It resolves virtual namespace
queries from carbon zipper into lists of real metrics.

Populating the index by sending messages
----------------------------------------
The search index is populated by consuming messages (via Kafka, HTTP API,
etc.).  There are 3 types of messages: metrics, tags, and custom.

Metric messages
---------------
Metric messages associate an arbitrary number of metrics with a value of a join key.

In this case, the join key is `fqdn`, and the join key value is `hostname-1234`.

    {
      "value": "hostname-1234",
      "metrics": [
        "server.hostname-1234.cpu.i7z"
      ],
      "key": "fqdn"
    }

Tag messages
------------
Tag messages associate an arbitrary number of tags with a value of a join key.

    {
      "value": "hostname-1234",
      "tags": [
        "server-state:live",
        "server-dc:lhr",
        "server-role:webserver"
      ],
      "key": "fqdn"
    }

Custom messages
---------------
Custom messages directly associate an arbitrary number of tags with an arbitrary number of metrics.

This allows humans to create custom groupings to easily search.

    {
      "metrics": [
        "monitors.lb_pool.www.lhr",
        "monitors.is_the_site_up",
        "server.hostname-1234.cpu.i7z"
      ],
      "tags": [
        "custom-favorites:monitoring"
      ]
    }


## TODO

1. monitoring/syslogging
2. simple snapshotting persistence (at least for the 'full' index, often added by humans)
3. some anti-entropy converging pressure of some kind
4. add `re-match` based on a proper text index: `re-filter` only narrows down
   results you have from other sources.
5. ???

...but it's complete enough to build indexes and serve search queries
from them.
