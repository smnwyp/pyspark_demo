#!/bin/bash


/usr/local/spark/bin/spark-submit pyspark_demo/step1_create_df.py
echo "step1 finished"
/usr/local/spark/bin/spark-submit pyspark_demo/step2_merge.py
echo "step2 finished"
