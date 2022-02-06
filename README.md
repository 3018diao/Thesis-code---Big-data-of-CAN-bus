## The execution process can be divided into the following steps:

1. Raw CAN-Trace comes from real cars or car test benches, collected by Datalogger.
2. Upload each datalogger’s CAN bus data to the Hadoop Distributed File System (HDFS).
3. Parsing raw CAN bus data from HDFS with Spark based on DBC file.
4. Writing of the parsed CAN bus data to the database.
5. Users can download raw trace directly from Hadoop Distributed File System (HDFS) or use SQL-query
to get data in the database
