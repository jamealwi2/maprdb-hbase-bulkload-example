# maprdb-hbase-bulkload-example

Aim:

To get familiarized with full/fresh bulk load and incremental bulk load concept in MapRDB tables. MapRDB is a high-performance NoSQL database. It has two main flavors – MapRDB Binary and MapRDB JSON. MapRDB Binary has the same capability as of Apache HBase, in fact, it comes with a lot of better performance and stability compared to Apache HBase.

Advantages of MapRDB bulk load over Apache HBase bulk load:

An HBase bulk load has these three steps:
[1] Generate HFiles using a mapreduce job. 
[2] Add the HFiles to the region servers. 
[3] Perform a full compaction to get good locality, since the previous step will not guarantee data locality.

A MapR-DB bulk load involves only one step: 
[1] Execute the mapreduce job. 

The MapRDB full bulk load writes directly to the segment files, bypassing the buckets. The incremental bulk load writes to the bucket files.  The mapreduce job cost for a MapRDB full bulk load is similar in cost to the one for HBase. However once complete, no compaction is needed with data locality in MapRDB. The mapreduce job for MapRDB incremental bulk load is more expensive than the mapreduce job for HBase bulk load since the MapRDB mapreduce job is actually updating the bucket files properly. Thus it is going to be slower than the HBase mapreduce job. But once complete, no compaction is needed with MapRDB for optimal performance.

Bulkload to MapRDB
The bulkload operation to MapRDB is useful when you are trying to load a large amount of data in one go. It avoids the normal write path and makes the operation much faster. It skips the write-ahead log (WAL). Definitely, this is the correct way to populate your MapRDB table (in that case, Apache HBase table as well) when you have huge put operation.

Full bulk load: Full bulk load on the MapRDB table is performed when the table does not contain any data. For this to happen, you need to make sure that ‘BULKLOAD’ property is set to ‘true’. You can set this property either at table creation time or using ‘alter’ command if the table is already created.

 a. When you create a table:
     create '<path_to_maprdb_table>', '<column_family>', BULKLOAD => 'true'
     E.g: create '/user/user01/blt', 'c', BULKLOAD => 'true'

 b. When table is already present:
     alter '<path_to_maprdb_table>', '<column_family>', BULKLOAD => 'true'
     E.g: alter '/user/user01/blt', 'c', BULKLOAD => 'false'

Incremental bulk load: Once there is data present in the MapRDB and table is enabled for read-write operation, then we need to do the incremental bulk load. The incremental bulk load can be executed multiple times. To enable incremental bulk load to a MapRDB table, make sure the ‘BULKLOAD’ property is set to ‘false’.

    alter '<path_to_maprdb_table>', '<column_family>', BULKLOAD => 'false'
    E.g: alter '/user/user01/blt', 'c', BULKLOAD => 'false'

You can use the same code for both full bulk load and incremental bulk load. The only change you have to make is to change property ‘BULKLOAD’ from ‘true’ (for full bulk load) to ‘false’ (for incremental bulk load).
