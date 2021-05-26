import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark {
  private var spark:SparkSession = null;

  def sparkRun():  SparkSession = {
    if (spark == null) {
      spark =
        SparkSession
          .builder
          .appName("Covid Data App")
          //.master("local")
          .config("spark.master", "local")
          .config("spark.eventLog.enabled", false)
          .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }
    spark
  }


  def loadData(s: String): DataFrame = {
    val mypath =
    s match {
      case "Confirmed World" =>
        "hdfs://localhost:9000/user/project2/time_series_covid_19_confirmed.csv"

      case "Confirmed US" =>
        "hdfs://localhost:9000/user/project2/time_series_covid_19_confirmed_US.csv"

      case "Deaths World" =>
        "hdfs://localhost:9000/user/project2/time_series_covid_19_deaths.csv"

      case "Deaths US" =>
        "hdfs://localhost:9000/user/project2/time_series_covid_19_deaths_US.csv"

      case "Recovered World" =>
        "hdfs://localhost:9000/user/project2/time_series_covid_19_recovered.csv"

      case "All Data" =>
        "hdfs://localhost:9000/user/project2/covid_19_data.csv"

      case _ => {
        println("Dataset not found, defaulting to all data")
        "hdfs://localhost:9000/user/project2/covid_19_data.csv"
      }


    }
    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(mypath)
    df
  }

}
