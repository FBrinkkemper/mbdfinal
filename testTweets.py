#!/usr/bin/env python
import sys
import os
import json
import pyspark

def doJob(rdd):
  return rdd.filter(rdd.coordinates.isNotNull()).select(rdd.coordinates.coordinates, rdd.coordinates.type, rdd.user.id, rdd.user.name).rdd

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("testTweets %s %s" % (in_dir, out_dir))
  # necessary prolog
  sc = pyspark.SparkContext(conf=conf)
  sqlContext = pyspark.sql.SQLContext(sc)
  # neceesary prolog for dataframes
  
  # add actual program using sc
  doJob(sqlContext.read.json(in_dir))\
    .saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
