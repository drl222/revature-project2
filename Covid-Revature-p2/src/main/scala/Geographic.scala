import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast, col, max}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.expressions.{Sin, ToRadians}
import org.apache.spark.sql.functions.{cos, expr, sin, toRadians}
import org.bouncycastle.crypto.io.CipherOutputStream

object Geographic {
  def geographicalData(spark: SparkSession): Unit = {
    val data1 = Spark.loadData("Confirmed World").createOrReplaceTempView("world")
    val data2 = Spark.loadData("All Data").createOrReplaceTempView("all")
    val data7 = Spark.loadData("Deaths World").createOrReplaceTempView("Deaths")

    val data3 = spark.sql("create or replace TEMPORARY view world_view as select count(w.`Country/Region`) as Num_of_Countries, " +
      "sum(cast(w.`5/2/21` as int)) as Confirmed_Cases, sum(cast(d.`5/2/21` as int)) as Deaths, case " +
      "when ((w.Lat between -90 and 0) AND (w.Long between 90 and 180)) then '(-90,0) & (90,180)' " +
      "when ((w.Lat between 15 and 50) AND (w.Long between 85 and 140)) then '(15,50) & (85,140)'" +
      "when ((w.Lat between 20 and 60) AND (w.Long between 0 and 80)) then '(20,60) & (0,80)'" +
      "when ((w.Lat between -40 and 40) AND (w.Long between -120 and 0)) then '(-40,40) & (-120,0)'" +
      "when ((w.Lat between -90 and 60) AND (w.Long between -90 and 20)) then '(-90,60) & (-90,20)'" +
      "when ((w.Lat between 0 and 90) AND (w.Long between 0 and 90)) then '(0,90) & (0,90)'" +
      "when ((w.Lat between -40 and 10) AND (w.Long between 20 and 120)) then '(-40,0) & (20,120)'" +
      "when ((w.Lat between 40 and 60) AND (w.Long between -180 and -90)) then '(40,60) & (-90,-180)' " +
      "else 'Not Labeled' END AS Latitude_and_Longitude from world as w join Deaths as d on " +
      "(w.`Country/Region` = d.`Country/Region` and w.`Province/State` IS NOT DISTINCT FROM d.`Province/State`) " +
      "group by `Latitude_and_Longitude` " +
      "order by Latitude_and_Longitude")
    val data8 = spark.sql("select *, round(Deaths / Confirmed_Cases * 100,2) as Death_Percentage from world_view").show
    //val data5 = spark.sql("select distinct `Country/Region`, sum(cast(Confirmed AS DOUBLE)), sum(cast(Deaths AS Double)) " +
    //  "from all group by `Country/Region`, cast(Confirmed AS DOUBLE), cast(Deaths AS DOUBLE) " +
    //  "order by Deaths DESC").show
   // val a1 = spark.sql("select ObservationDate, `Country/Region`, confirmed from all order by `Country/Region` " +
   //   "where ObservationDate = 02/28/2020").show



/*

    val data4 = spark.sql("select cast(w.Lat AS DOUBLE) AS Latitude, " +
      "cast(w.Long AS DOUBLE) AS Longitude, sum(cast(distinct `5/2/21` AS int)) AS Sum_Confirmed, " +
      "cast(a.Confirmed as double) AS Confirmed_Cases, cast(a.Deaths as DOUBLE)" +
      "from all AS a join world as w on a.`Country/Region` = w.`Country/Region` " +
      "where ((w.lat BETWEEN 15.0 AND 80.0) AND (w.long BETWEEN -180.0 AND -20.0)) " +
      "group by cast(w.Lat as DOUBLE), cast(w.Long AS DOUBLE), cast(a.Confirmed AS DOUBLE), cast(a.Deaths AS DOUBLE)" +
      "order by cast(a.Confirmed as Double) DESC").show

<<<<<<< HEAD
*/
=======



>>>>>>> 2c923cbcd77a1fdc846c78eee7cc0c8f04c01e79
    //val latData = spark.sql("select a.Last Update, a.Confirmed AS cases, w.Country/Region, w.Lat AS Latitude FROM total AS a join on world AS w on w.Country/Region = a.Country/Region")//.show(10)
    //where Lat between 35 and 45").show
    //between start date and end date
    //val data4 = spark.sql("select sum(cast(`5/2/21` AS int)) AS This_Date, first(cast(w.Lat AS DOUBLE, false)) AS Latitude, first(cast(w.long AS DOUBLE, false)) " +
    //  "AS Longitude, first(a.Confirmed, false) " +
    //  "AS Confirmed_Cases, from all AS a join world as w on a.`Country/Region` = w.`Country/Region` " +
    //  "where ((w.lat BETWEEN 20.0 AND 30.0) AND (w.long BETWEEN 100.0 AND 140.0))").show

    //dense_rank() over (PARTITION BY w.lat ORDER BY w.long DESC) as rank
    /*
  val lat1 = COS(ToRadians(Latitude))
  val long1 = COS(ToRadians(Longitude))
   val geo_group = spark.sql(select *,(((acos(sin((Lat*pi()/180)) * sin((Lat*pi()/180))
          +cos((Lat*pi()/180))*cos((Lat*pi()/180))*cos((Long*pi()/180))))*
          180/pi())*637100) as Lat_long FROM world AS geo_data group by Lat_Long order by Lat_Long ASC).show(10)

  val data4 = spark.sql("select w.Lat AS Latitude, w.Long AS Longitude, a.Last Update, a.Confirmed " +
  "AS Confirmed_Cases, 6371000 * DEGREES(ACOS(COS(RADIANS(Latitude)) * COS(RADIANS(Longitude)) " + COS(RADIAN())
  "from world AS w JOIN ON ")
*/

  }
}
