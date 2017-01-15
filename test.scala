import magellan.{Point, Polygon, PolyLine}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class TwitterRecord(userId: String, latitude: String, longitude: String, point: Point)

/* Read in TwitterData */
val twitter = sc.textFile("data/test.tsv").map { line =>
val parts = line.split("\t" )
val userId = parts(0)
val latitude = parts(2)
val longitude = parts(1)
val point = Point(parts(2).toDouble, parts(1).toDouble)
TwitterRecord(userId, latitude, longitude, point)
}.
repartition(100).
toDF().
cache()

val twitter = sqlContext.read.json("data/1000tweets.json")
case class TwitterRecord(userId: String, point: Point)
val tweets = twitter.filter("coordinates is not null").select($"user.id",$"coordinates.type", $"coordinates.coordinates"(0).as("latitude"), $"coordinates.coordinates"(1).as("longitude")).map{ line => 
val userId = line(0).toString
val latitude = line(2).toString
val longitude = line(3).toString
val point = Point(latitude.toDouble, longitude.toDouble)
TwitterRecord(userId, point)
}.
repartition(100).
toDF().
cache()

	
/* Read in Postcode data */
val postcodes = sqlContext.read.format("magellan").
load("data/WSG/").
select($"polygon", explode($"metadata").as(Seq("k", "v"))).
where($"k" === "PC4").
withColumnRenamed("v", "postalcode").
drop("k").
cache()


/* Combine the data */ 
val joined = postcodes.
join(tweets).
where($"point" within $"polygon").
drop("polygon").drop("point").
cache()


/* Determine postalcode for each user
TODO Filer postalcodes */
val user_with_postalcode = joined


/* Count users per postalcode */
val users_per_postalcode = user_with_postalcode.
groupBy($"postalcode").
agg(countDistinct("userId").
as("#users")).
orderBy(col("#users").desc).
cache()

users_per_postalcode.show(5)
