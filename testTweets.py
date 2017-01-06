#!/usr/bin/env python
import sys
import os
import json
import pyspark

def doJob(tweets):
  #return rdd.filter(rdd.coordinates.isNotNull()).select(rdd.coordinates.coordinates, rdd.coordinates.type, rdd.user.id, rdd.user.name).rdd
  def getBestCoordinate(pair):
    id, coordinates = pair
    return id, [round(coordinates[0][0], 2),round(coordinates[0][1], 2)]

  #Krijgt alle tweets met geotags
  return tweets.filter(tweets.coordinates.isNotNull()).filter(tweets.coordinates.type == "Point").select(tweets.user.id, tweets.coordinates.coordinates).rdd.groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : getBestCoordinate(x))
  
  #return tweets.filter(tweets.coordinates.isNotNull()).filter(tweets.coordinates.type == "Point").select(tweets.user.id, tweets.coordinates.coordinates).rdd.reduceByKey(lambda c1,c2: c1+c2)

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
