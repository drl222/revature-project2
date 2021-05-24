import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future
object Main {



    def main(args: Array[String]): Unit = {
      val spark =
        SparkSession
          .builder
          .appName("Hello Spark App")
          //.master("local")
          .config("spark.master", "local")
          .config("spark.eventLog.enabled", false)
          .getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")




      val df = spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load("hdfs://localhost:9000/user/project2/time_series_covid_19_deaths_US.csv")
      df.createOrReplaceTempView("people")
      df.registerTempTable("people")
      val ff = spark.sql("select admin2, Combined_key,cast(`5/2/21` as int) from people order by `5/2/21` DESC").show
      //val sqlDF = spark.sql("SELECT Combined_Key ,max(`5/2/21`) as maxDeaths from people group by Combined_Key order by maxDeaths DESC limit 1" )
      //sqlDF.show()





      spark.stop()

    }

}
