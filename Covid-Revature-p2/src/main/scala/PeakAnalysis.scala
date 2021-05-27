import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object PeakAnalysis {
  def subtract_and_halve(a:Seq[Double], b:Seq[Double]):Seq[Double] = {
    (a,b).zipped.map({
      case (elementa, elementb) => (elementa - elementb)/2.0
    })
  }

  def takeDerivative(rdd:RDD[Seq[Double]]): RDD[Seq[Double]] = {
    rdd.map(vec =>{
      // central finite difference
      val all_but_first_and_last = subtract_and_halve(vec.slice(2, vec.size), vec.slice(0, vec.size-2))
      // for the first and last point, because we can't do a central finite difference, do a forward/backwards one instead
      val first_point = vec(1) - vec(0)
      val last_point = vec(vec.size-1) - vec(vec.size-2)
      first_point +: all_but_first_and_last :+ last_point
    })
  }

  def findPeak(spark:SparkSession): Unit = {
    import spark.implicits._
    val df = Spark.loadData("Confirmed World")
    val metadata_only = df.select("Province/State", "Country/Region", "Lat", "Long").rdd
    val data_only = df.drop("Province/State", "Country/Region", "Lat", "Long").rdd
      .map(row =>row.toSeq.map(x => x.asInstanceOf[String].toDouble))
    val daily_cases = takeDerivative(data_only)


    val derivative = takeDerivative(daily_cases)
    val second_derivative = takeDerivative(derivative)

    derivative.take(12).foreach(x => println(x))
    println()
    second_derivative.take(12).foreach(x => println(x))

    val peaks_indices = derivative.zip(second_derivative).map({
      case (deriv1, deriv2) => {
        val zipped = deriv1.zip(deriv2).zipWithIndex
        zipped.filter({
          case ((d1, d2), i) => math.abs(d1) < 0.01 && d2 < -0.01
          // first derivative must be zero and the second derivative must be negative for this to be a peak
        }).map(_._2)
      }
    })

    val peak_numbers = peaks_indices.zip(daily_cases).zip(metadata_only).map({
      case ((peaks_row, daily_cases_row), metadata_only_row) => {
        val peak_cases = peaks_row.map({
          index => (index, daily_cases_row(index))
        }).toList
        val location_name = metadata_only_row.getAs[String]("Province/State") + ", " + metadata_only_row.getAs[String]("Country/Region")
        (location_name, peak_cases)
      }
    })

    peak_numbers.foreach({case (label, peakinfo) => println(s"${label}: ${peakinfo}")})
  }
}