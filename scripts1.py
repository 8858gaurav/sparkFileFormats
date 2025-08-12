# row based file format: where data are stored row by row on the disk
# coluns based file format: all the entire columns are stored together on the disk, column by columns.

# integer takes less spaces as compared to the string data type, if we are storing the same data type at one place,
# then it will take less space as compared to the row based file formats. all the same data types are stored together
# in the columns based file formats, so it will take less space how ? - by using lightweight compression techniques.
# e.g -> dictionary encoding, run length encoding, bit packing, run length encoding, etc.

# generalized compression techniques are: snappy, gzip, bzip2, lz4, zstd, etc.

# row based file format are faster to write, columns based file format when we want to read a subest of columns. 

# for heavy write operation, use row based file formats: AVRO
# for heavy read operations, use columns based file formats: parquet (compatibel with Spark), ORC (comaptible with Hive)

# text based file format are: json, csv, & XML. XML, & json is worse than the csv format. json will take more space, its' obvious
# multiline json format files, &  are not splittable, but json format files are splittable, if every records coming on a new line.
# if the files are not splittable (num partitions = 1), then we'll not get any parallelism, we'll not be able to utilize our cpu respurces.

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .appName("FileFormats") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
    # everything is string by default in csv format files.
    orders_schema = "order_id string , order_date string, customer_id string,order_status string"
    orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/datasets/orders/orders_1gb.csv") \

    orders_df.printSchema()
    # root
    #  |-- order_id: string (nullable = true)
    #  |-- order_date: string (nullable = true)
    #  |-- customer_id: string (nullable = true)
    #  |-- order_status: string (nullable = true)

    # if we changed the datatype, then it will take some time to print the schema, it's a type conversion.
    order_schema = "order_id long , order_date date, customer_id long,order_status string"
    order_df = spark.read \
    .format("csv") \
    .schema(order_schema) \
    .load("/public/trendytech/datasets/orders/orders_1gb.csv") 

    order_df.printSchema()
    # root
    #  |-- order_id: long (nullable = true)
    #  |-- order_date: date (nullable = true)
    #  |-- customer_id: long (nullable = true)
    #  |-- order_status: string (nullable = true)
        
    order_df.rdd.getNumPartitions() # 9
    # means this files are splittable, we are able to split this file into a multiple parts, we'll get parallelism here.

    orders_df.write.format('json').mode("overwrite").save("output101")
    # from 128 mb, it'll becomes 269 MB. Json take more space. 
    # [itv020752@g01 ~]$ hadoop fs -ls -h output101
    # Found 10 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-08-11 09:13 output101/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:12 output101/part-00000-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:12 output101/part-00001-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:12 output101/part-00002-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:12 output101/part-00003-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:13 output101/part-00004-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:13 output101/part-00005-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:13 output101/part-00006-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    269.1 M 2025-08-11 09:12 output101/part-00007-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # -rw-r--r--   3 itv020752 supergroup    102.7 M 2025-08-11 09:11 output101/part-00008-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json

    # here each records is coming on one line, so this kind of json files are splittable.
    # [itv020752@g01 ~]$ hadoop fs -head output101/part-00000-e69dd4f8-cbf2-4bdc-b960-56aa4d6a8a78-c000.json
    # {"order_id":1,"order_date":"2013-07-25","customer_id":11599,"order_status":"CLOSED"}
    # {"order_id":2,"order_date":"2013-07-25","customer_id":256,"order_status":"PENDING_PAYMENT"}
    # {"order_id":3,"order_date":"2013-07-25","customer_id":12111,"order_status":"COMPLETE"}
    # {"order_id":4,"order_date":"2013-07-25","customer_id":8827,"order_status":"CLOSED"}
    # {"order_id":5,"order_date":"2013-07-25","customer_id":11318,"order_status":"COMPLETE"}
    # {"order_id":6,"order_date":"2013-07-25","customer_id":7130,"order_status":"COMPLETE"}
    # {"order_id":7,"order_date":"2013-07-25","customer_id":4530,"order_status":"COMPLETE"}
    # {"order_id":8,"order_date":"2013-07-25","customer_id":2911,"order_status":"PROCESSING"}
    # {"order_id":9,"order_date":"2013-07-25","customer_id":5657,"order_status":"PENDING_PAYMENT"}
    # {"order_id":10,"order_date":"2013-07-25","customer_id":5648,"order_status":"PENDING_PAYMENT"}
    # {"order_id":11,"order_date":"2013-07-25","customer_id":918,"order_status":"PAYMENT_REVIEW"}

    orders_json_df = spark.read.format("json").load("output101")
    orders_json_df.rdd.getNumPartitions() # 18
    # earlier it was 9 files under this locations: output101.

    # this file is of 6 MB, here each records is not coming on one line, it's an array of json elements.
    # [itv020752@g01 ~]$ hadoop fs -head /public/trendytech/datasets/json_sample_multiline
    # [{"order_id":1,"order_date":"2013-07-25","customer_id":11599,"order_status":"CLOSED"},
    #  {"order_id":2,"order_date":"2013-07-25","customer_id":256,"order_status":"PENDING_PAYMENT"},
    #  {"order_id":3,"order_date":"2013-07-25","customer_id":12111,"order_status":"COMPLETE"},
    #  {"order_id":4,"order_date":"2013-07-25","customer_id":8827,"order_status":"CLOSED"},
    #  {"order_id":5,"order_date":"2013-07-25","customer_id":11318,"order_status":"COMPLETE"},
    #  {"order_id":6,"order_date":"2013-07-25","customer_id":7130,"order_status":"COMPLETE"},
    #  {"order_id":7,"order_date":"2013-07-25","customer_id":4530,"order_status":"COMPLETE"},
    #  {"order_id":8,"order_date":"2013-07-25","customer_id":2911,"order_status":"PROCESSING"},
    #  {"order_id":9,"order_date":"2013-07-25","customer_id":5657,"order_status":"PENDING_PAYMENT"},
    #  {"order_id":10,"order_date":"2013-07-25","customer_id":5648,"order_status":"PENDING_PAYMENT"},
    #  {"order_id":11,"order_date":"2013-07-25","customer_id":918,"order_status":"PAYMENT_REVIEW"}]
    orders_json_ml_df = spark.read.format('json').option('multiline', True) \
    .load('/public/trendytech/datasets/json_sample_multiline')
    orders_json_ml_df.rdd.getNumPartitions() # 1
    orders_json_ml_df.show()
    # +-----------+----------+--------+---------------+
    # |customer_id|order_date|order_id|   order_status|
    # +-----------+----------+--------+---------------+
    # |      11599|2013-07-25|       1|         CLOSED|
    # |        256|2013-07-25|       2|PENDING_PAYMENT|
    # |      12111|2013-07-25|       3|       COMPLETE|
    # |       8827|2013-07-25|       4|         CLOSED|
    # |      11318|2013-07-25|       5|       COMPLETE|
    # |       7130|2013-07-25|       6|       COMPLETE|
    # |       4530|2013-07-25|       7|       COMPLETE|
    # |       2911|2013-07-25|       8|     PROCESSING|
    # |       5657|2013-07-25|       9|PENDING_PAYMENT|
    # |       5648|2013-07-25|      10|PENDING_PAYMENT|
    # |        918|2013-07-25|      11| PAYMENT_REVIEW|
    # |       1837|2013-07-25|      12|         CLOSED|
    # |       9149|2013-07-25|      13|PENDING_PAYMENT|
    # |       9842|2013-07-25|      14|     PROCESSING|
    # |       2568|2013-07-25|      15|       COMPLETE|
    # |       7276|2013-07-25|      16|PENDING_PAYMENT|
    # |       2667|2013-07-25|      17|       COMPLETE|
    # |       1205|2013-07-25|      18|         CLOSED|
    # |       9488|2013-07-25|      19|PENDING_PAYMENT|
    # |       9198|2013-07-25|      20|     PROCESSING|
    # +-----------+----------+--------+---------------+

    # now change the partition size from 128 MB to 1.2 Mb, since our multiline file size is 6 MB
    spark.conf.get('spark.sql.files.maxPartitionBytes') # 134217728b
    spark.conf.set('spark.sql.files.maxPartitionBytes', '1342177b') # 1.2 MB

    orders_json_ml_df = spark.read.format('json').option('multiline', True) \
    .load('/public/trendytech/datasets/json_sample_multiline')
    orders_json_ml_df.rdd.getNumPartitions() # 1, earlier it was 1 also.


    # now checking for the normal csv file formats. 
    spark.read \
    .format("csv") \
    .schema(order_schema) \
    .load("/public/trendytech/datasets/orders/orders_1gb.csv") \
    .rdd.getNumPartitions() # earlier it was 9, now it becomes 839 partitions.

    # that's why multiline json format files are not recommended to use in the big Data.

    # avro, orc, & parquet formats files are splittable, all this kind of files support schema evolutions. We can
    # use any kind of compression techniques with this file formats. When dealing with Json, csv, & XMl - we are restricted to use
    # certain compression techniques. Avro is a row based, while parquet (spark), & ORC (Hive) is a columns based file formats.