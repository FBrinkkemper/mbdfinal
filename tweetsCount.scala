package nl.utwente.bigdata; // don't change package name

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import magellan.{Point, Polygon, PolyLine}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.immutable.ListMap

// uncomment if your program uses sql
import org.apache.spark.sql.{ SQLContext }

// uncomment if your program uses sql
//import org.apache.spark.sql.{ SQLContext }

object userCount {

  case class TwitterRecord(userId: String, point: Point)
  case class PostalCodesList(userId: String, postalCodes: Iterable[String])
  case class PostalCodesMap(userId: String, postalCodes: Map[String, Int])
  case class PostalCodesListMap(userId: String, postalCodes: ListMap[String, Int])
  case class PostalCode(userId: String, postalCode: String)

  /*
   add actual program here, start by specifying
   the input and output types in RDD[X]
   */
  def doJob(twitter: org.apache.spark.sql.DataFrame, sqlContext: SQLContext) : RDD[org.apache.spark.sql.Row] = {

    import sqlContext.implicits._
    //val tweets = twitter.rdd.map(x => ("Key", 1)).reduceByKey((x,y) => (x+y)).repartition(1)
		val tweets = twitter.select(twitter("user.id").as("userId"), twitter("coordinates").isNotNull.as("coordinates"))
      .rdd.map(x => (x(0), (1, if(x(1).asInstanceOf[Boolean]) 1 else 0))).reduceByKey((x,y) => (1, x._2.max(y._2))).map(x => ("Key", x._2)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    return tweets.map(x => Row(x))

    /*.filter(twitter("coordinates").isNotNull).select(twitter("user.id"),twitter("coordinates.type"), twitter("coordinates.coordinates")(0).as("latitude"), twitter("coordinates.coordinates")(1).as("longitude")).map{ line =>
		val userId = line(0).toString
		val latitude = line(2).toString
		val longitude = line(3).toString
		val point = Point(latitude.toDouble, longitude.toDouble)
    TwitterRecord(userId, point)
  }.repartition(100).toDF().
		cache()*/


		/* Read in Postcode data */
		/*val loadedPostcodes = sqlContext.read.format("magellan").load("WSG/")
    val postcodes = loadedPostcodes.select(loadedPostcodes("polygon"), org.apache.spark.sql.functions.explode(loadedPostcodes("metadata")).as(Seq("k", "v"))).cache()
    val filteredPostcodes = postcodes.where(postcodes("k") === "PC4").
		withColumnRenamed("v", "postalcode").
		drop("k").
		cache()
*/
		/* Combine the data */
		/*val joined = filteredPostcodes.
		join(tweets).
		where(tweets("point") within filteredPostcodes("polygon")).
		drop("polygon").drop("point").
		cache()
*/
		/* Determine postalcode for each user (userId, postalCode)
		TODO Filer postalcodes */
    //.map(x => (x._0, List(x._1))) .map(lambda x : getBestCoordinate(x))

		/*val user_with_postalcode = joined.rdd.map(x => (x(1), x(0))).groupByKey()
      .map(x => Row(x._2.groupBy(identity).mapValues(_.size).maxBy(_._2)._1, x._1)).cache()


      .map(x => PostalCodesList(x(0).asInstanceOf[String], x(1).asInstanceOf[Iterable[String]]))
      .map(x => PostalCodesMap(x.userId, x.postalCodes.groupBy(identity).mapValues(_.size)))
      .map(x => PostalCodesListMap(x.userId, ListMap(x.postalCodes.toSeq.sortWith(_._2 > _._2):_*)))
      .map(x => Row(x.userId, x.postalCodes.head._1))

      map{pair =>
      val userId = pair(0)
      val list = pair(1).asInstanceOf[Iterable[String]]
      val map = list.groupBy(identity).mapValues(_.size)
      val listMap = ListMap(map.toSeq.sortWith(_._2 > _._2):_*)
      Row(userId, listMap.head._1)
}.cache()*/

/*
val pair = Row(x)


val map = list.groupBy(identity).mapValues(_.size)
val listMap = ListMap(map.toSeq.sortWith(_._2 > _._2):_*)
Row(userId, listMap.head._1)*/

    //return user_with_postalcode.repartition(1)
    //.groupBy($"userId").map{ x => return getPostalCode(x)}

    /*groupBy($"postalcode").
		agg(countDistinct("userId").
		as("#users")).
		orderBy(col("#users").desc)*/

		/* Count users per postalcode */
		/*val users_per_postalcode = user_with_postalcode.map(x => (x(0), 1)).reduceByKey((x, y) => x + y).map(x => Row(x))
		.cache()

		return users_per_postalcode.repartition(1)*/
  }

  def main(args: Array[String]) {
    // command line arguments
    val appName = this.getClass.getName

    // interpret command line, default: first argument is input second is output
    val inputDir = args(0)
    val outputDir = args(1)

    // configuration
    val conf = new SparkConf()
      .setAppName(s"$appName $inputDir $outputDir")

    // create spark context
    val sc = new SparkContext(conf)

    // uncomment if your program uses sql
    val sqlContext = new SQLContext(sc)

    // potentially
    doJob(sqlContext.read.json(inputDir), sqlContext).saveAsTextFile(outputDir)

  }
}
