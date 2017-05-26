### v0.16.1 - May 26, 2017
---
##### Misc/Bugs
* replace heap-based intersect with pairwise (minor performance improvement)
* respect `text_index_service` config value everywhere (minor bug fix)
* stop timing \/admin\/* endpoints (more accurate `requests_in_*_to_*` Graphite metric reporting)

### v0.16.0 - March 2, 2017
---
##### Features
* Support for protobuf3

##### Misc/Bugs
* siphash -> sip13 for hashing tags/metrics (minor performance improvement)
* fix missing pprof HTTP routes (`/debug/pprof/{heap|profile|block|trace}`)
* stop materializing and release memory back to the OS once graceful restart is triggered: this creates space for the new instance to load up

### v0.15.0 - Feb 6, 2017
---
##### Features
* Support for quoting pieces of `text-match` queries using `<` and `>`. This allows selecting pieces of metric that span dots, like so: `text-match:<df.root.bytes_free>`. This requires a `carbonapi` equal to or newer than [79f91477a](https://github.com/dgryski/carbonapi/commit/79f91477a4e0c985c4af18bf98c8ed3bfb465cec).
* Graceful restart using a `facebookgo/grace`. Send SIGUSR2 to spawn a new carbonsearch. The old process will hand over to the new one once the new one has warmed up.
* Warmup period: carbonsearch will wait until it has ingested enough data to serve search queries. This is managed by the `warm_threshold` config setting on a per-consumer basis. This may be bypassed by passing `-coldStart` on the command line.

##### Config
* `warm_threshold`: a ratio (valid range: 0 to 1) that describes how much progress a consumer should make before considering itself warm. For the Kafka consumer this represents position in the buffer relative to first offset seen; the HTTP consumer must be informed of progress using a POST to `/consumer/progress`.

##### Misc/Bugs
* index materialization timings are now only logged under debug builds

### v0.14.0 - Dec 5, 2016
---
This is the first tagged release, so changes are from previously deployed version ([28fbc853](https://github.com/kanatohodets/carbonsearch/commit/28fbc853753f742347afbb9acf577f6996e360b4)).

##### Features
* autocomplete for queries that have a trailing glob. When deployed alongside carbonzipper [64a4933](https://github.com/dgryski/carbonzipper/commit/64a493343d91081a371340211f4a518b01fdff36), carbonsearch queries can be auto-completed from the graphite-web UI.

##### Config
* change default search prefix to 'virt.v1.*.' for better graphite-web support ([49b6a91](https://github.com/kanatohodets/carbonsearch/commit/49b6a910b7e546876c1d9b9495d1ff73e0fe11ab) for details)
* support prefix in config file (rather than a CLI flag)
* split indexes mapping now associates multiple services with a single index ([link](https://github.com/kanatohodets/carbonsearch/commit/7a3e3c1fb45869f315e974ded99fa1471a7586e8))

##### Reporting
* new graphite metrics for index generation timings and index sizes
* request logging for all requests, query time + result counts for Find requests

##### Misc/Bugs
* various small tweaks to make carbonsearch clean against [go-staticcheck](https://github.com/dominikh/go-staticcheck), other cleanups based on various other linters/static checkers
* include BuildVersion in ExpVars
