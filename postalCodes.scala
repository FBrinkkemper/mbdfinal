package nl.utwente.bigdata; // don't change package name

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import magellan.{Point, Polygon, PolyLine}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// uncomment if your program uses sql
import org.apache.spark.sql.{ SQLContext }

object postalCodes {

  case class TwitterRecord(userId: String, point: Point)

  /*
   add actual program here, start by specifying
   the input and output types in RDD[X]
   */
  def doJob(twitter: org.apache.spark.sql.DataFrame, sqlContext: SQLContext) : RDD[org.apache.spark.sql.Row] = {

    import sqlContext.implicits._
		val tweets = twitter.filter(twitter("coordinates").isNotNull).select(twitter("user.id"),twitter("coordinates.type"), twitter("coordinates.coordinates")(0).as("latitude"), twitter("coordinates.coordinates")(1).as("longitude")).map{ line =>
		val userId = line(0).toString
		val latitude = line(2).toString
		val longitude = line(3).toString
		val point = Point(latitude.toDouble, longitude.toDouble)
    TwitterRecord(userId, point)
  }.repartition(100).toDF().
		cache()


		/* Read in Postcode data */
		val loadedPostcodes = sqlContext.read.format("magellan").load("WSG/")
    val postcodes = loadedPostcodes.select(loadedPostcodes("polygon"), org.apache.spark.sql.functions.explode(loadedPostcodes("metadata")).as(Seq("k", "v"))).cache()
    val filteredPostcodes = postcodes.where(postcodes("k") === "PC4").
		withColumnRenamed("v", "postalcode").
		drop("k").
		cache()

		/* Combine the data */
		val joined = filteredPostcodes.
		join(tweets).
		where(tweets("point") within filteredPostcodes("polygon")).
		drop("polygon").drop("point").
		cache()

		/* Determine postalcode for each user (userId, postalCode)
		TODO Filer postalcodes */
		val user_with_postalcode = joined
    //.groupBy($"userId").map{ x => return getPostalCode(x)}

		/* Count users per postalcode */
		val users_per_postalcode = user_with_postalcode.
		groupBy($"postalcode").
		agg(countDistinct("userId").
		as("#users")).
		orderBy(col("#users").desc).
		cache()

		return users_per_postalcode.rdd.repartition(1)
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
