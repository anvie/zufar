# ZufarDB

A [Memcached](http://memcached.org/) alternative, that support auto-sharding for scalability and semi-persistent
durability, where the data is asynchronously transfered from memory to disk backed by [RocksDB](http://rocksdb.org/).

## Features

* Decentralized P2P with no single point of failure.
* Semi-persistent.
* Memcached compatible.

You can use any memcached client library out there. At this moment only support for `set`, `get`, and `delete` command.

**NOTE: not production ready, this is my personal research to learning Rust.**
