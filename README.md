# mongodb-charglt
Mongo implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt), but on MongoDN

It Has the  basic functionality of charglt, but is missing stuff  - see TODO
## TODO
 
* Feed to downstream systems
* Make scale - currently is sync and one thread
* Running totals currently do a doc-by-doc scan instead of a single call.

## Status

While this is fine to play with, it's not a fair representation of MongoDB at the moment.


