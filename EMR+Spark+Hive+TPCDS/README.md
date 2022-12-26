Reference: 
https://github.com/hdinsight/tpcds-hdinsight

PLEASE REPLACE "tpcdsspark.inventoryorc" and "tpcdsspark.inventoryparq" under reader and writer header
****************************************
a. ##pyspark program#
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.appName('emrfsoptcom')\
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
.config("hive.exec.dynamic.partition", "true")\
.config("hive.exec.dynamic.partition.mode", "nonstrict")\
.config("spark.sql.hive.convertMetastoreOrc", "true")\
.config("spark.sql.hive.convertMetastoreParquet", "true")\
.config("spark.sql.parquet.output.committer.class", "com.amazon .emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter")\
.config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")\
.config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true").enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")

##reader
SourceTableDDL = spark.sql("SHOW CREATE TABLE tpcdsspark.inventoryorc")
SourceTableDDL.show (10000, False)
partDf = spark.sql("SELECT * FROM tpcdsspark.inventoryorc WHERE inv_warehouse_sk='2'")

##writer
DestinationTableDDL = spark.sql("SHOW CREATE TABLE tpcdsspark.inventoryparq")
DestinationTableDDL.show (10000, False)
writeDf = partDf.write.mode("overwrite").option("partitionOverwriteMode", "static").format("parquet").insertInto("tpcdsspark.inventoryparq")


b. ##source table (orc)##
"tpcdsspark.inventoryorc"

CREATE EXTERNAL TABLE `inventoryorc`(
  `inv_date_sk` int COMMENT '', 
  `inv_item_sk` int COMMENT '', 
  `inv_warehouse_sk` int COMMENT '', 
  `inv_quantity_on_hand` bigint COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io .orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io .orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io .orc.OrcOutputFormat'
LOCATION
  's3://sklargedataset/tpcdsorc/inventoryorc/'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'orc.compress'='SNAPPY', 
  'presto_query_id'='20210318_182424_00001_xuur5')


c.  ##destination table (parquet)##
"tpcdsspark.inventoryparq"

CREATE EXTERNAL TABLE `inventoryparq`(
  `inv_date_sk` int COMMENT '', 
  `inv_item_sk` int COMMENT '', 
  `inv_warehouse_sk` int COMMENT '', 
  `inv_quantity_on_hand` bigint COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io .parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io .parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io .parquet.MapredParquetOutputFormat'
LOCATION
  's3://sklargedataset/tpcdsparq/inventoryparq/'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'has_encrypted_data'='false', 
  'numFiles'='0', 
  'numRows'='-1', 
  'presto_query_id'='20210318_184815_00001_iinn4', 
  'rawDataSize'='-1', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"inv_date_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\"}},{\"name\":\"inv_item_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\"}},{\"name\":\"inv_warehouse_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"\"}},{\"name\":\"inv_quantity_on_hand\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"\"}}]}', 
  'totalSize'='0')

****************************************
