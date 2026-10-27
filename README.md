# mongodb-charglt
Mongo implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt), but on MongoDB

It Has the  basic functionality of charglt, but is missing stuff  - see TODO

## How it uses MongoDB

Each user is one document in `CHARGLT_DB.CHARGLT_USERS`, keyed by user id.

* The benchmarks use the MongoDB reactive streams driver, with Project Reactor to chain the steps together.
  A single thread sends requests at `tpms`, and responses arrive on the driver's threads, which update
  each user's client-side state. A user with a request outstanding is skipped until it finishes, and at
  the end each program waits for everything outstanding to finish.
* Every change to a user is a read-modify-write inside a transaction with majority write concern. If
  MongoDB reports a transient transaction error, such as a write conflict with another client, it's
  retried with a new session.
* At most `MAX_IN_FLIGHT` requests are outstanding at once (default 64). On a single node in Docker
  Desktop, 1 gave ~640 TPS, 64 gave ~4,100 TPS, and 256 gave less throughput with much worse latency.
* Running totals are a single aggregation, and clearing old reservations is a single `updateMany`.
* Dates are stored as epoch ms, so documents don't depend on the JVM's locale.

## Running

You need Java 21+, Maven, and a MongoDB replica set, because the code uses transactions. For a single
node replica set in Docker:

    docker run -d --name mongo-rs -p 27017:27017 mongo:8.0 --replSet rs0 --bind_ip_all
    docker exec mongo-rs mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'

Build with `mvn package`, then run, with `target/classes` and the dependencies on the classpath:

| Class | Parameters |
|---|---|
| `ie.rolfe.mongodbcharglt.CreateChargingDemoData` | hostnames recordcount tpms maxinitialcredit |
| `ie.rolfe.mongodbcharglt.ChargingDemoTransactions` | hostnames recordcount tpms durationseconds queryseconds |
| `ie.rolfe.mongodbcharglt.ChargingDemoKVStore` | hostnames recordcount tpms durationseconds queryseconds jsonsize deltaProportion |
| `ie.rolfe.mongodbcharglt.DeleteChargingDemoData` | hostnames recordcount tpms |

Documents written by versions before epoch ms dates can't be read - delete and recreate the users.

## See Also:

* [aerospike-charglt](https://github.com/srmadscience/aerospike-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)
* [s2-charglt](https://github.com/srmadscience/s2-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [oracle-charglt](https://github.com/srmadscience/oracle-charglt)
* [postgres-charglt](https://github.com/srmadscience/postgres-charglt)

## TODO
 
* Feed to downstream systems

## Status

While this is fine to play with, it's not a fair representation of MongoDB at the moment.


