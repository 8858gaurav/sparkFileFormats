# By default snappy compression techniques will used for parquet, orc, & avro file formats.
# in terms of space, csv -> avro -> parquet -> orc

# for the same 1 gb file Orders files, we'll get 9 partitions, the partition size is 128 Mb (csv), 30MB(Avro), 13MB(parquet), 7MB(ORC)

# why parquet/ORC run faster

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
    
    orders_schema = "order_id long , order_date string, customer_id long, order_status string"
    orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/datasets/orders/orders_1gb.csv")
    print(orders_df.rdd.getNumPartitions()) # 9

    orders_df.write.mode("overwrite").option('path', 'data/datasets/parquet/').save()
    # or orders_df.write.mode("overwrite").save("data/datasets/parquet/")
    orders_df.write.format('orc').mode("overwrite").save("data/datasets/orc/")

    # default it's a snappy compression techniques for orc, avro, & parquet
    # [itv020752@g01 ~]$ hadoop fs -ls -h data/datasets/orc/
    # Found 10 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-08-12 03:12 data/datasets/orc/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:12 data/datasets/orc/part-00000-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:11 data/datasets/orc/part-00001-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:11 data/datasets/orc/part-00002-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:12 data/datasets/orc/part-00003-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:12 data/datasets/orc/part-00004-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:12 data/datasets/orc/part-00005-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:12 data/datasets/orc/part-00006-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.6 M 2025-08-12 03:12 data/datasets/orc/part-00007-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup      6.4 M 2025-08-12 03:11 data/datasets/orc/part-00008-9db63c75-dd7b-4dcb-a421-11f8aa9d3432-c000.snappy.orc


    # [itv020752@g01 ~]$ hadoop fs -ls -h data/datasets/parquet/
    # Found 10 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-08-12 03:11 data/datasets/parquet/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:10 data/datasets/parquet/part-00000-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:10 data/datasets/parquet/part-00001-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:11 data/datasets/parquet/part-00002-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:11 data/datasets/parquet/part-00003-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:11 data/datasets/parquet/part-00004-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:11 data/datasets/parquet/part-00005-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:11 data/datasets/parquet/part-00006-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup     13.4 M 2025-08-12 03:11 data/datasets/parquet/part-00007-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # -rw-r--r--   3 itv020752 supergroup      5.3 M 2025-08-12 03:11 data/datasets/parquet/part-00008-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet

    # u will see metadata at the end of the file in json format. i.e orc, parquet, & avro called as a self describing data.

    # [itv020752@g01 ~]$ hadoop fs -tail data/datasets/parquet/part-00000-b1332549-4eca-4e6a-b726-a5e2e850658e-c000.snappy.parquet
    # #\H
    #    spark_schemorder_id
    #                       %
    # order_date%%
    #             customer_id
    #                        %
    # ,0&���                  order_status%ؚ�order_idؚ�������<
    #       5
    # order_dateؚ�����&���<6(2014-07-24 00:00:00.02013-07-25 00:00:00.0,�&���5
    #                                                                        customer_idؚ�������&�����,0&���

    #                                                                                                       5
    #                                                                                                       order_statusؚ�������&���
    #                                                                                                                               <6(SUSPECTED_ؚ�,org.apache.spark.version3.1.2)org.apache.spark.sql.parquet.row.metadata�{"type":"struct","fields":[{"name":"order_id","type":"long","nullable":true,"metadata":{}},{"name":"order_date","type":"string","nullable":true,"metadata":{}},{"name":"customer_id","type":"long","nullable":true,"metadata":{}},{"name":"order_status","type":"string","nullable":true,"metadata":{}}]}Jparquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1)L�PAR1

    # each parquet file contains -> row groups (row group size = 128 MB) -> columns chunks (= # of columns) -> page (page size = 1MB)
    # row group contains the meta data at tabular view (max, minx, etc for every columns ), page contains the meta data at columnar level (max, min, etc for each columns)

    # creating a file (paruquet > 128 MB) to check whether the # of row group will increase or not

    # how our query runs faster for parquet, orc, and avro - it will check whether this row group conatins this order_id or not,
    # if it's contains then it will lok which page contains the values of this order_id.
    # if it's not, then it will go for another row group, then it will check at page level.
    # make sure we are just skipping or entering into any row group by looking the meta data only, if your filters values
    # lies in the meta data range (row group meta data, or page level meta data), then it will go inside it (row group or page level)
    # we are doing predicates push down, and columns push down.

    orders_schema = "order_id long , order_date string, customer_id long, order_status string"
    orders_df_new = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/retail_db/ordersnew/*")
    print(orders_df_new.rdd.getNumPartitions()) # 23

    orders_df_new.coalesce(1).write.mode("overwrite").option('path', 'data/datasets/parquetnew').save()
    # [itv020752@g01 ~]$ parquet-tools meta  data/datasets/parquetnew/part-00000-a3beb71f-fbc2-48ee-8af0-96ac258ded2d-c000.snappy.parquet
    # file:         hdfs://m01.itversity.com:9000/user/itv020752/data/datasets/parquetnew/part-00000-a3beb71f-fbc2-48ee-8af0-96ac258ded2d-c000.snappy.parquet 
    # creator:      parquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1) 
    # extra:        org.apache.spark.version = 3.1.2 
    # extra:        org.apache.spark.sql.parquet.row.metadata = {"type":"struct","fields":[{"name":"order_id","type":"long","nullable":true,"metadata":{}},{"name":"order_date","type":"string","nullable":true,"metadata":{}},{"name":"customer_id","type":"long","nullable":true,"metadata":{}},{"name":"order_status","type":"string","nullable":true,"metadata":{}}]} 

    # file schema:  spark_schema 
    # -------------------------------------------------------------------------------------------------------------------------------------------
    # order_id:     OPTIONAL INT64 R:0 D:1
    # order_date:   OPTIONAL BINARY O:UTF8 R:0 D:1
    # customer_id:  OPTIONAL INT64 R:0 D:1
    # order_status: OPTIONAL BINARY O:UTF8 R:0 D:1

    # row group 1:  RC:34970100 TS:135286469 OFFSET:4 
    # -------------------------------------------------------------------------------------------------------------------------------------------
    # order_id:      INT64 SNAPPY DO:0 FPO:4 SZ:65758292/65845577/1.00 VC:34970100 ENC:RLE,BIT_PACKED,PLAIN_DICTIONARY
    # order_date:    BINARY SNAPPY DO:0 FPO:65758296 SZ:5296348/8065663/1.52 VC:34970100 ENC:RLE,BIT_PACKED,PLAIN_DICTIONARY
    # customer_id:   INT64 SNAPPY DO:0 FPO:71054644 SZ:61337161/61375193/1.00 VC:34970100 ENC:RLE,BIT_PACKED,PLAIN_DICTIONARY
    # order_status:  BINARY SNAPPY DO:0 FPO:132391805 SZ:38/36/0.95 VC:34970100 ENC:RLE,BIT_PACKED,PLAIN

    # row group 2:  RC:28282701 TS:116859316 OFFSET:134217728 
    # -------------------------------------------------------------------------------------------------------------------------------------------
    # order_id:      INT64 SNAPPY DO:0 FPO:134217728 SZ:53074332/56107189/1.06 VC:28282701 ENC:RLE,BIT_PACKED,PLAIN_DICTIONARY
    # order_date:    BINARY SNAPPY DO:0 FPO:187292060 SZ:5812750/11082734/1.91 VC:28282701 ENC:RLE,BIT_PACKED,PLAIN_DICTIONARY
    # customer_id:   INT64 SNAPPY DO:0 FPO:193104810 SZ:46876926/49669357/1.06 VC:28282701 ENC:RLE,BIT_PACKED,PLAIN_DICTIONARY
    # order_status:  BINARY SNAPPY DO:0 FPO:239981736 SZ:38/36/0.95 VC:28282701 ENC:RLE,BIT_PACKED,PLAIN

    # meaning of this
    # SZ:65758292/65845577/1.00 = SZ:Compressed_size/Actual_size/Level_of_compression or Compression_Ratio
    # From 1.9GB (CSV), it will become 228.9MB in parquet formats.

    #[itv020752@g01 ~]$ hadoop fs -du -h /public/trendytech/retail_db
    #1.9 G    5.8 G    /public/trendytech/retail_db/ordersnew


    # [itv020752@g01 ~]$ hadoop fs -ls -h data/datasets/parquetnew
    # Found 2 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-08-12 03:46 data/datasets/parquetnew/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup    228.9 M 2025-08-12 03:46 data/datasets/parquetnew/part-00000-a3beb71f-fbc2-48ee-8af0-96ac258ded2d-c000.snappy.parquet


    # parquet-data/ (Folder)
    # │
    # ├── File 1: part-00000.parquet (128 MB on disk)
    # │   ├── Row Group 1 (~128 MB raw data in RAM -> compressed down to e.g., ~42 MB)
    # │   │   ├── Column Chunk 1 (order_id)       --> Pages 1, 2, 3... (~1 MB each)
    # │   │   ├── Column Chunk 2 (order_date)     --> Pages 1, 2, 3... (~1 MB each)
    # │   │   └── Column Chunk 3 (customer_id)    --> Pages 1, 2, 3... (~1 MB each)
    # │   │
    # │   ├── Row Group 2 (~128 MB raw data in RAM -> compressed down to e.g., ~43 MB)
    # │   │   └── [Same Column Chunks & Pages structure]
    # │   │
    # │   └── Row Group 3 (~128 MB raw data in RAM -> compressed down to e.g., ~43 MB)
    # │       └── [Same Column Chunks & Pages structure]
    # │
    # └── File 2: part-00001.parquet (128 MB on disk)
    #     ├── Row Group 1 (~128 MB raw data in RAM -> compressed down to e.g., ~42 MB)
    #     │   └── [Same Column Chunks & Pages structure]
    #     ├── Row Group 2 (~128 MB raw data in RAM -> compressed down to e.g., ~43 MB)
    #     │   └── [Same Column Chunks & Pages structure]
    #     └── Row Group 3 (~128 MB raw data in RAM -> compressed down to e.g., ~43 MB)
    #         └── [Same Column Chunks & Pages structure]

    # [STEP 0: SOURCE DISK]            [STEP 1: EXECUTOR RAM]               [STEP 2: RAM BUFFER]               [STEP 3: TARGET DISK]
    # Source Data                      Uncompressed Row Processing          Encoded & Compressed               Flushed to Parquet File
    # --------------------             ---------------------------          --------------------               -----------------------
    # CSV / JSON / Parquet  ──(Read)──>   ~128 MB Raw Records        ──(Compress)──> ~42 MB Byte Buffer   ──(Flush)──>    Row Group written to
    # on HDFS / S3 / Local                in Executor Memory                       in Executor Memory                 part-00000.parquet
    
    # [State]                          [State]                              [State]                            [State]
    # • Serialized Bytes               • Deserialized Binary               • Columnar Encoded                 • Persisted Parquet
    #   (Text/CSV layout)                (`UnsafeRow` format)                 (Dictionary/RLE + Snappy)          Row Group on Disk

    # Component,   Step 0: Source Input (Est.),Step 1: Uncompressed Size in RAM,Step 2: Encoded Buffer in RAM,Step 3: Compressed Size written to DISK
    # Row Group 1, ~600 MB CSV text,           ~128 MB Raw Records,             ~42 MB Snappy Buffer,        ~42 MB inside part-00000.parquet
    # Row Group 2, ~600 MB CSV text,           ~128 MB Raw Records,             ~43 MB Snappy Buffer,        ~43 MB inside part-00000.parquet
    # Row Group 3, ~600 MB CSV text,           ~128 MB Raw Records,             ~43 MB Snappy Buffer,        ~43 MB inside part-00000.parquet
    # Total File,  ~1.8 GB CSV Input,          ~384 MB Total Raw Data,          ~128 MB Total Buffer,        ~128 MB Single File on Disk
    # (part-00000.parquet)
