--注：Hbase大小写敏感
--创建表，列族为info
create 'test0818','info'
--插入数据
--执行rowkey
put 'test0818','001','info:id','1'
put 'test0818','001','info:name','A'
put 'test0818','001','info:age','20'

put 'test0818','002','info:id','2'
put 'test0818','002','info:name','B'
put 'test0818','002','info:age','30'

put 'test0818','003','info:id','3'
put 'test0818','003','info:name','C'
put 'test0818','003','info:age','28'
--查询
--查询指定数据
get 'test0818','001'
get 'test0818','001','info:id'
--遍历数据
scan 'test0818'
--查询行数
count 'test0818'
--删除表
--删除指定数据
delete 'test0818','001','info:id'
--删除行
deleteall 'test0818','001'
--删除所有数据
truncate 'test0818'