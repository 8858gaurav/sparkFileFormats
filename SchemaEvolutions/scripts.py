# schema evolutions
# folder directory = schema_evolution (data/datasets/evolSchema)
# 1 files has 2 columns in directory: schema_evolution, another files has 3 columns in the same directory: schema_evolution
# , again another file has 1 or 4 columns in the same directory: schema_evolution
# , again another files is available whose structure is different (ordering of the columns)

# when we enable the schema evolution, then we'll not face an any erros: .option('mergeSchema', True)
# it will handle whether the directory has same schema in all the files or not
# it will not work, if the data type changed for any columns, e.g file 1 has order_id in integer format, all the other
# files should have a integer format for order_id. it will work - if this columns (order_id) will not present then it will show u
# a NULL values.

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
           .appName("schemaEvolutions") \
           .config("spark.shuffle.useOldFetchProtocol","true") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    
    # [itv020752@g01 ~]$ hadoop fs -ls -h /public/trendytech/datasets/parquet-schema-evol-demo/csv
    # Found 4 items
    # -rw-r--r--   3 itv005857 supergroup         49 2023-08-20 19:29 /public/trendytech/datasets/parquet-schema-evol-demo/csv/orders1.csv
    # -rw-r--r--   3 itv005857 supergroup         60 2023-08-20 19:29 /public/trendytech/datasets/parquet-schema-evol-demo/csv/orders2.csv
    # -rw-r--r--   3 itv005857 supergroup         78 2023-08-20 19:29 /public/trendytech/datasets/parquet-schema-evol-demo/csv/orders3.csv
    # -rw-r--r--   3 itv005857 supergroup         78 2023-08-20 20:01 /public/trendytech/datasets/parquet-schema-evol-demo/csv/orders4.csv

    df_1_schema = 'order_id long, order_date date'
    df_1 = spark.read.format("csv").schema(df_1_schema).load("/public/trendytech/datasets/parquet-schema-evol-demo/csv/orders1.csv")
    # +--------+----------+
    # |order_id|order_date|
    # +--------+----------+
    # |       1|2013-07-25|
    # |       2|2013-07-25|
    # +--------+----------+

    df_2_schema = 'order_id long, order_date date, customer_id long'
    df_2 = spark.read.format("csv").schema(df_2_schema).load("/public/trendytech/datasets/parquet-schema-evol-demo/csv/orders2.csv")
    # +--------+----------+-----------+
    # |order_id|order_date|customer_id|
    # +--------+----------+-----------+
    # |       3|2013-07-25|      12111|
    # |       4|2013-07-25|       8827|
    # +--------+----------+-----------+

    df_3_schema = 'order_id long, order_date date, customer_id long, order_status string'
    df_3 = spark.read.format("csv").schema(df_3_schema).load("/public/trendytech/datasets/parquet-schema-evol-demo/csv/orders3.csv")
    # +--------+----------+-----------+------------+
    # |order_id|order_date|customer_id|order_status|
    # +--------+----------+-----------+------------+
    # |       5|2013-07-25|      11318|    COMPLETE|
    # |       6|2013-07-25|       7130|    COMPLETE|
    # +--------+----------+-----------+------------+

    df_4_schema = 'order_id long, order_date date, order_status string, customer_id long'
    df_4 = spark.read.format("csv").schema(df_4_schema).load("/public/trendytech/datasets/parquet-schema-evol-demo/csv/orders4.csv")
    # +--------+----------+------------+-----------+
    # |order_id|order_date|order_status|customer_id|
    # +--------+----------+------------+-----------+
    # |       5|2013-07-25|    COMPLETE|      11318|
    # |       6|2013-07-25|    COMPLETE|       7130|
    # +--------+----------+------------+-----------+

    # writing to a locations:
    df_1.write.mode("overwrite").option('path', 'data/datasets/evolSchema').save()
    df_2.write.mode("append").save('data/datasets/evolSchema')
    df_3.write.mode("append").option('path', 'data/datasets/evolSchema').save()
    df_4.write.mode("append").save('data/datasets/evolSchema')

    df_schema_evol = spark.read.option('mergeSchema', True).load("data/datasets/evolSchema/*")

    # +--------+----------+------------+-----------+
    # |order_id|order_date|order_status|customer_id|
    # +--------+----------+------------+-----------+
    # |       5|2013-07-25|    COMPLETE|      11318|
    # |       6|2013-07-25|    COMPLETE|       7130|
    # |       5|2013-07-25|    COMPLETE|      11318|
    # |       6|2013-07-25|    COMPLETE|       7130|
    # |       3|2013-07-25|        null|      12111|
    # |       4|2013-07-25|        null|       8827|
    # |       1|2013-07-25|        null|       null|
    # |       2|2013-07-25|        null|       null|
    # +--------+----------+------------+-----------+