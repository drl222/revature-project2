import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast, col, max}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.expressions.{Sin, ToRadians}
import org.apache.spark.sql.functions.{cos, expr, sin, toRadians}
import org.bouncycastle.crypto.io.CipherOutputStream

object Geographic {
  def geographicalData(spark: SparkSession): Unit ={
    val data1 = Spark.loadData("Confirmed World").createOrReplaceTempView("world")
    val data2 = Spark.loadData("All Data").createOrReplaceTempView("all")


    val data4 = spark.sql("select cast(w.Lat AS DOUBLE) AS Latitude, " +
      "cast(w.Long AS DOUBLE) AS Longitude, sum(cast(`5/2/21` AS int)) AS Sum_Confirmed, " +
      "cast(a.Confirmed as double) AS Confirmed_Cases, cast(a.Deaths as DOUBLE)" +
      "from all AS a join world as w on a.`Country/Region` = w.`Country/Region` " +
      "where ((w.lat BETWEEN 15.0 AND 80.0) AND (w.long BETWEEN -180.0 AND -20.0)) " +
      "group by cast(w.Lat as DOUBLE), cast(w.Long AS DOUBLE), cast(a.Confirmed AS DOUBLE), cast(a.Deaths AS DOUBLE)" +
      "order by cast(a.Confirmed as Double) DESC").show




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
