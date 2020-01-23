# Journal

### Features

- All operations required by the Akka Persistence @extref:[journal plugin API](akka:scala/persistence.html#journal-plugin-api) are fully supported.
- The plugin uses Cassandra in a pure log-oriented way i.e. data is only ever inserted but never updated (deletions are made on user request only).
- Writes of messages are batched to optimize throughput for `persistAsync`. See @extref:[batch writes](akka:scala/persistence.html#batch-writes) for details how to configure batch sizes. The plugin was tested to work properly under high load.
- Messages written by a single persistent actor are partitioned across the cluster to achieve scalability with data volume by adding nodes.
- @extref:[Persistence Query](akka:scala/persistence-query.html) support by `CassandraReadJournal`


### Schema 

The keyspace and tables needs to be created before using the plugin. 
  
@@@ warning

Auto creation of the keyspace and tables
is included as a development convenience and should never be used in production. Cassandra does not handle
concurrent schema migrations well and if every Akka node tries to create the schema at the same time you'll
get column id mismatch errors in Cassandra.

@@@

The default keyspace used by the plugin is `akka`, it should be created with the
NetworkTopology replication strategy with a replication factor of at least 3:

```
CREATE KEYSPACE IF NOT EXISTS akka WITH replication = {'class': 'NetworkTopologyStrategy', '<your_dc_name>' : 3 }; 
```

For local testing you can use the following:

@@snip [journal-schema](/target/journal-keyspace.txt) { #journal-keyspace } 

There are multiple tables required. These need to be created before starting your application.
For local testing you can enable `cassnadra-plugin.journal.table-autocreate`. The default table definitions look like this:

@@snip [journal-tables](/target/journal-tables.txt) { #journal-tables } 

#### Messages table

A description of the important columns.

| Column            | Description                                                                                                                                              |
|:------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------|
| persistence_id    | The persistence id                                                                                                                                       |
| partition_nr      | Artificial partition key to stop Cassandra partitions growing too large                                                                                  |
| sequence_nr       | Sequence nr of the event                                                                                                                                 |
| timestamp         | Timeuuid of the event, used by events by tag                                                                                                             |
| timebucket        | Artificial partition key to keep the tag_views table partitions growing too large. It is in this table as the tags query used to use a materialized view |
| write_uuid        | To detect concurrent writes for the same persistence id                                                                                                  |
| ser_id            | serializer id of the serializer used for the event blob                                                                                                  |
| ser_manifest      | serializer manifest of the serializer used for the event blob                                                                                            |
| event_manifest    |                                                                                                                                                          |
| event_blob        |                                                                                                                                                          |
| meta_ser_id       |                                                                                                                                                          |
| meta_ser_manifest |                                                                                                                                                          |
| meta              |                                                                                                                                                          |
| message           |                                                                                                                                                          |
| tags              |                                                                                                                                                          |










#### Tag views table

### Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "cassandra-plugin.journal"

This will run the journal with its default settings. The default settings can be changed with the configuration properties defined in [reference.conf](https://github.com/akka/akka-persistence-cassandra/blob/master/core/src/main/resources/reference.conf):

One important setting is to configure the database driver to retry the initial connection:

`datastax-java-driver.advanced.reconnect-on-init = true`

It is not enabled automatically as it is in the driver's reference.conf and is not overridable in a profile.

#### Consistency

By default the journal uses `QUORUM` for all reads and writes.
For setups with multiple datacentres this can set to `LOCAL_QUORUM` to
avoid cross DC latency.
Any other consistency level is highly discouraged.

```
datastax-java-driver.profiles {
  cassandra-plugin {
    basic.request.consistency = QUORUM
  }
}
```
 
#### Journal settings

@@Snip [reference.conf](/core/src/main/resources/reference.conf) { #journal }

#### Cassandra driver overrides

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #profile }

#### Shared settings for all parts of the plugin

The following settings are shared by the `journal`, `query`, and `snapshot` parts of the plugin and are under
`cassandra-plugin`: 

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #shared }

Events by tag configuration is under `cassandra-plugin-events-by-tag` and shared
b `journal` and `query`.

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #events-by-tag }

>>>>>>> Stashed changes

### Caveats

- Detailed tests under failure conditions are still missing.
- Range deletion performance (i.e. `deleteMessages` up to a specified sequence number) depends on the extend of previous deletions
    - linearly increases with the number of tombstones generated by previous permanent deletions and drops to a minimum after compaction
- For versions prior to 0.80 events by tag uses Cassandra Materialized Views which are a new feature that has yet to stabilise
  Use at your own risk, see [here](https://lists.apache.org/thread.html/d81a61da48e1b872d7599df4edfa8e244d34cbd591a18539f724796f@%3Cdev.cassandra.apache.org%3E) for a discussion on the Cassandra dev mailing list.
  Version 0.80 and on migrated away from Materialized Views and maintain a separate table for events by tag queries.


These issues are likely to be resolved in future versions of the plugin.

### Default keyspace and table definitions

```
CREATE KEYSPACE IF NOT EXISTS akka
WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 };

CREATE TABLE IF NOT EXISTS akka.messages (
  persistence_id text,
  partition_nr bigint,
  sequence_nr bigint,
  timestamp timeuuid,
  timebucket text,
  writer_uuid text,
  ser_id int,
  ser_manifest text,
  event_manifest text,
  event blob,
  meta_ser_id int,
  meta_ser_manifest text,
  meta blob,
  message blob,
  tags set<text>,
  PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp, timebucket))
  WITH gc_grace_seconds =864000
  AND compaction = {
    'class' : 'SizeTieredCompactionStrategy',
    'enabled' : true,
    'tombstone_compaction_interval' : 86400,
    'tombstone_threshold' : 0.2,
    'unchecked_tombstone_compaction' : false,
    'bucket_high' : 1.5,
    'bucket_low' : 0.5,
    'max_threshold' : 32,
    'min_threshold' : 4,
    'min_sstable_size' : 50
    };

CREATE TABLE IF NOT EXISTS akka.tag_views (
  tag_name text,
  persistence_id text,
  sequence_nr bigint,
  timebucket bigint,
  timestamp timeuuid,
  tag_pid_sequence_nr bigint,
  writer_uuid text,
  ser_id int,
  ser_manifest text,
  event_manifest text,
  event blob,
  meta_ser_id int,
  meta_ser_manifest text,
  meta blob,
  PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, tag_pid_sequence_nr))
  WITH gc_grace_seconds =864000
  AND compaction = {
    'class' : 'SizeTieredCompactionStrategy',
    'enabled' : true,
    'tombstone_compaction_interval' : 86400,
    'tombstone_threshold' : 0.2,
    'unchecked_tombstone_compaction' : false,
    'bucket_high' : 1.5,
    'bucket_low' : 0.5,
    'max_threshold' : 32,
    'min_threshold' : 4,
    'min_sstable_size' : 50
    };

CREATE TABLE IF NOT EXISTS akka.tag_write_progress(
  persistence_id text,
  tag text,
  sequence_nr bigint,
  tag_pid_sequence_nr bigint,
  offset timeuuid,
  PRIMARY KEY (persistence_id, tag));

CREATE TABLE IF NOT EXISTS akka.tag_scanning(
  persistence_id text,
  sequence_nr bigint,
  PRIMARY KEY (persistence_id));

CREATE TABLE IF NOT EXISTS akka.metadata(
  persistence_id text PRIMARY KEY,
  deleted_to bigint,
  properties map<text,text>);
```
