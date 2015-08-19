# ZufarDB

Decentralized key-value store that support auto-sharding for scalability and semi-persistent
durability, where the data is asynchronously transfered from memory to disk backed by [RocksDB](http://rocksdb.org/).

## Features

* Decentralized P2P with no single point of failure.
* Semi-persistent.
* Memcached compatible, 

ZufarDB could be [Memcached](http://memcached.org/) alternative, you can use any memcached client library out there. At this moment only support for `set`, `get`, and `delete` command.

## Compile

You need [Rust](https://www.rust-lang.org/) compiler nightly, tested @ 1.3.0 (38517944f 2015-08-03)).

Rocksdb library:

```bash
wget https://github.com/facebook/rocksdb/archive/rocksdb-3.8.tar.gz
tar xvf rocksdb-3.8.tar.gz && cd rocksdb-rocksdb-3.8 && make shared_lib
sudo make install
```

And then build:

```bash
$ cargo build
```

## Test drive

```bash
$ ./target/debug/zufar serve example/node1.conf
```

Run more node:

```bash
$ ./target/debug/zufar serve example/node2.conf &
$ ./target/debug/zufar serve example/node3.conf &
```




etc.

Check cluster status:

```bash
$ ./target/debug/zufar status 127.0.0.1 8123
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address              Load       GUID                              Rack
UN  127.0.0.1:8123         0/10      0                                1
UN  127.0.0.1:9123         0/7       1                                1
UN  127.0.0.1:7123         0/2       2                                1
```

Looks familiar? Yeah but this is not Cassandra, ignore rack, we don't support network topology and multi-dc yet.


## Write/Read/Delete

Test CRUD operation via telnet (using [memcached protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt))

```bash
$ telnet localhost 8122
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
set hello 0 0 5
world
STORED
get hello
VALUE hello 0 5 
world
END
del hello
DELETED
get hello
END
```

With more detail like where the record is stored and how long the operation taken, just add postfix `d` for each command:

```bash
setd abc123 0 0 6
def456
STORED
node-2 in 0ms
getd abc123
VALUE abc123 0 6 
def456
END
node-2 in 0ms
```




**NOTE: not production ready, this is my personal research to learning Rust.**
