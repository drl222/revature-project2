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
      "when ((w.Lat between -90 and 0) AND (w.Long between 90 and 180)) then 'Australia/Pacific Islands' " +
      "when ((w.Lat between 0 and 90) AND (w.Long between 90 and 180)) then 'East/Southeast Asia'" +
      "when ((w.Lat between 40 and 90) AND (w.Long between -25 and 90)) then 'Europe/Central Asia'" +
      "when ((w.Lat between 0 and 40) AND (w.Long between -25 and 90)) then 'North Africa/Middle East/South Asia'" +
      "when ((w.Lat between -90 and 0) AND (w.Long between -25 and 90)) then 'Southern Africa'" +
      "when ((w.Lat between -90 and 10) AND (w.Long between -180 and -25)) then 'South America'" +
      "when ((w.Lat between 10 and 90) AND (w.Long between -180 and -25)) then 'North America'" +
      "else 'Not Labeled' END AS Latitude_and_Longitude from world as w join Deaths as d on " +
      "(w.`Country/Region` = d.`Country/Region` and w.`Province/State` IS NOT DISTINCT FROM d.`Province/State`) " +
      "group by `Latitude_and_Longitude` " +
      "order by Latitude_and_Longitude")
    val data8 = spark.sql("select *, round(Deaths / Confirmed_Cases * 100,2) as Death_Percentage from world_view").show







  }
}
