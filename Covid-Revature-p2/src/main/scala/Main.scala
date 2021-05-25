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


//      Trends.run()

      val spark = Spark.sparkRun()
//      val s = "Deaths US"

      //Spark.loadData(s).createOrReplaceTempView("people")
      //val ff = spark.sql("select admin2, Combined_key,cast(`5/2/21` as int) from people order by `5/2/21` DESC").show
      //val sqlDF = spark.sql("SELECT Combined_Key ,max(`5/2/21`) as maxDeaths from people group by Combined_Key order by maxDeaths DESC limit 1" )
      //sqlDF.show()

//      PeakAnalysis.findPeak(spark)
      PeakAnalysis.findPeakV2(spark)





     spark.close()

    }

}
