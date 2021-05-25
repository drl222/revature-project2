import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Sin, ToRadians}
import org.apache.spark.sql.functions.{cos,sin,toRadians}
import org.bouncycastle.crypto.io.CipherOutputStream

object Geographic {
  def geographicalData(spark: SparkSession): Unit ={
    val data1 = Spark.loadData("Confirmed World").createOrReplaceTempView("world")
    val data2 = Spark.loadData("All Data").createOrReplaceTempView("all")
    //val latData = spark.sql("select a.Last Update, a.Confirmed AS cases, w.Country/Region, w.Lat AS Latitude FROM total AS a join on world AS w on w.Country/Region = a.Country/Region")//.show(10)
    //where Lat between 35 and 45").show

    val data4 = spark.sql("select cast(w.Lat AS DOUBLE) AS Latitude, cast(w.long AS DOUBLE) " +
      "AS Longitude, a.`Last Update`, a.Confirmed " +
      "AS Confirmed_Cases from all AS a join world as w where a.`Country/Region` = w.`Country/Region`").show
      /*
    val lat1 = COS(ToRadians(Latitude))

    val long1 = COS(ToRadians(Longitude))
    val res = Sin(lat1/2) * Sin(long1/2)
            + Cos(ToRadians(lat1))
            * Sin(long1/2) * sin(long1/2)

    val data4 = spark.sql("select w.Lat AS Latitude, w.Long AS Longitude, a.Last Update, a.Confirmed " +
    "AS Confirmed_Cases, 6371000 * DEGREES(ACOS(COS(RADIANS(Latitude)) * COS(RADIANS(Longitude)) " + COS(RADIAN())
    "from world AS w JOIN ON ")

*/

  }
}
