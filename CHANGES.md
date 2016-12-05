### 0.14 - Dec 5, 2016
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
