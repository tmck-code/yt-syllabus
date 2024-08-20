#!/usr/bin/env python3

import pyspark

spark = pyspark.sql.SparkSession.builder.master('local[1]').getOrCreate()

df = spark.read.json('a.ndjson')

print([r.asDict(recursive=True) for r in df.collect()])
