#!/bin/bash

/spark/bin/spark-submit --master spark://spark-master:7077 app.py \
    hdfs://namenode:9000/dir_proj_1_small 38 40 -120 -104 2010-10-12 2010-10-31
