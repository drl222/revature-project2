// run inside spark-shell with expanded memory:
// spark-shell --driver-memory 1g

val ratings_rdd = sc.textFile("/user/project2/practice/ratings.dat").map(_.split("::"))
// userid, movieid, rating, timestamp
val users_rdd = sc.textFile("/user/project2/practice/users.dat").map(_.split("::"))
// userid, gender, age, ?, zipcode
val movies_rdd = sc.textFile("/user/project2/practice/movies.dat").map(_.split("::"))
// movieid, movietitle, moviegenres (separated by |)

// What are the top 10 most viewed movies?

val popular_movie_ids = ratings_rdd.groupBy(_(1)).map({ case(x,y) => (x.toLong, y.size) }).sortBy(_._2, false).take(10)
// ALTERNATIVELY:
// object TupleOrdering extends Ordering[(Long, Int)] {
//   import scala.math.Ordered.orderingToOrdered
//   def compare(a:(Long, Int), b:(Long, Int)) = (b._2, b._1) compare (a._2, a._1)
// }
// ratings_rdd.groupBy(_(1)).map({ case(x,y) => (x.toLong, y.size) }).takeOrdered(10)(TupleOrdering)
val movies_by_id = movies_rdd.groupBy(_(0)).map({ case(movieid,row) => (movieid.toLong, row) })
val final_answer = sc.parallelize(popular_movie_ids).join(movies_by_id).sortBy({ case(movieid, (count, row)) => count }, false).map({ case(movieid, (count, row)) => (row.head)(1) }).collect
final_answer.foreach(println)

// What are the distinct list of genres available?
val distinct_genres = movies_rdd.flatMap(row => row(2).split('|')).distinct().collect

// How many movies for each genre?
val genre_count = movies_rdd.flatMap(row => row(2).split('|')).groupBy(x => x).map({ case(genre, list) => (genre, list.size) }).sortBy(_._1)

// How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
val count_by_starting_letter = movies_rdd.map(_(1).toUpperCase()(0)).countByValue()

val date = raw"(\d{4})-(\d{2})-(\d{2})".r
"2004-01-20" match {
  case date(year, month, day) => s"$year was a good year for PLs."
}

// List the latest released movies
val movie_year_re = raw"(.*)\((\d+)\)".r
// input: "Toy Story (1995)"
// output: ("Toy Story","1995")
val movie_year_rdd = movies_rdd.map(_(1)).map({
	x => x match {
		case movie_year_re(title, year) => (title.trim(), year.toLong)
		case _ => (x, 0L)
	}
})
val latest_releases = movie_year_rdd.sortBy(_._2, false).take(10)

// Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
val ratings_df = spark.read.option("delimiter","::").option("inferSchema", true).format("csv").load("/user/project2/practice/ratings.dat").toDF("userid", "movieid", "rating", "timestamp")
ratings_df.registerTempTable("ratings")
val users_df = spark.read.option("delimiter","::").option("inferSchema", true).format("csv").load("/user/project2/practice/users.dat").toDF("userid", "gender", "age", "favoritenumber", "zipcode")
users_df.registerTempTable("users")
val movies_df = spark.read.option("delimiter","::").option("inferSchema", true).format("csv").load("/user/project2/practice/movies.dat").toDF("movieid", "movietitle", "moviegenres")
movies_df.registerTempTable("movies")

// Find the list of the oldest released movies.
spark.sql("""SELECT SUBSTR(movietitle, 0, LENGTH(movietitle) - 6) AS title,
	CAST(SUBSTR(movietitle, LENGTH(movietitle) - 4, 4) AS int) AS year
	FROM movies ORDER BY YEAR ASC LIMIT 20""").show

// How many movies are released each year?
spark.sql("""SELECT year, count(year) AS num_movies_released FROM
	(SELECT SUBSTR(movietitle, 0, LENGTH(movietitle) - 6) AS title,
	CAST(SUBSTR(movietitle, LENGTH(movietitle) - 4, 4) AS int) AS year
	FROM movies) S
	GROUP BY year ORDER BY num_movies_released DESC""").show

// How many number of movies are there for each rating?
spark.sql("""SELECT rating, count(rating) AS num_movies FROM ratings
	GROUP BY rating ORDER BY rating""").show

// How many users have rated each movie?
// assuming a user cannot rate a movie twice
spark.sql("""SELECT SUBSTR(M.movietitle, 0, LENGTH(movietitle) - 6) AS title, R.num_movies FROM
	(SELECT movieid, count(movieid) AS num_movies
		FROM ratings
		GROUP BY movieid ORDER BY movieid
	) R INNER JOIN movies M ON R.movieid = M.movieid ORDER BY num_movies DESC""").show

// What is the total rating for each movie?
spark.sql("""SELECT SUBSTR(M.movietitle, 0, LENGTH(movietitle) - 6) AS title, R.total_rating FROM
	(SELECT movieid, sum(rating) AS total_rating
		FROM ratings
		GROUP BY movieid ORDER BY movieid
	) R INNER JOIN movies M ON R.movieid = M.movieid ORDER BY total_rating DESC""").show

// What is the average rating for each movie?
spark.sql("""SELECT SUBSTR(M.movietitle, 0, LENGTH(movietitle) - 6) AS title, R.avg_rating FROM
	(SELECT movieid, avg(rating) AS avg_rating
		FROM ratings
		GROUP BY movieid ORDER BY movieid
	) R INNER JOIN movies M ON R.movieid = M.movieid ORDER BY avg_rating DESC""").show


// DATAFRAMES
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val movies_df2 = movies_df.withColumn("title", $"movietitle".substr(lit(0), length($"movietitle") - 6)).withColumn("year", $"movietitle".substr(length($"movietitle") - 4, lit(4))).withColumn("genres_list", split($"moviegenres", """\|""")).select($"movieid", $"title", $"year", $"genres_list").show

val users_df2 = users_df

val ratings_schema = StructType(Array(
  StructField("userid", IntegerType, true),
  StructField("movieid", IntegerType, true),
  StructField("rating", IntegerType, true),
  StructField("timestamp", LongType, true))
)

val ratings_df = spark.read.option("delimiter","::").schema(ratings_schema).format("csv").load("/user/project2/practice/ratings.dat")
