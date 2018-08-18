CREATE TABLE impala_kudu.test0818 (
   rowkey STRING ,
   id INT NULL,
   name STRING NULL,
   age INT NULL,
   PRIMARY KEY (rowkey)
 )
 PARTITION BY HASH (rowkey) PARTITIONS 16
 STORED AS KUDU
 TBLPROPERTIES (
 'kudu.master_addresses'='bigdata02:7051,bigdata03:7051,bigdata04:7051,bigdata05:7051,bigdata06:7051,bigdata07:7051,bigdata08:7051',
 'kudu.table_name'='test0818');

upsert into impala_kudu.test0818 values ('001',1,'A',20);
upsert into impala_kudu.test0818 values ('002',2,'B',30);
upsert into impala_kudu.test0818 values ('003',3,'C',26);


