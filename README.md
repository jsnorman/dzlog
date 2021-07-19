## 数据采集平台
从kafka 中消费数据写入hive表中，一般很容想到用spark，flink消费数据写入hive表中。业务需要申请kafka topic，再写spark/flink 任务，把数据写入hive 表，过程比较长。如果需要采集应用日志很多，启动的spark/flink任务相应也很多
为了平台化解决数据采集问题，用户只需要填写topic 和要写入hive表名，写入数据自动完成，dzlog不依赖spark/flink，是一个spring boot 应用，基于spring kafka 同时消费多个topic并发消费数据，补数据多个节点分布式消费数据，增加吞吐能力。

## dzlog 规则
1. 每十五分钟一个分区，按照消息接收时间作为区分。
2. 只写入原始数据，不做解析，用户单独跑ETL任务解析数据。
3. 支持parquet、orc、iceberg三种格式。
4. 自动合并碎片文件。
