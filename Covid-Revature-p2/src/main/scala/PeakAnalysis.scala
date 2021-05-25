import org.apache.spark.sql.SparkSession
import breeze.linalg._
import org.apache.spark.rdd.RDD

object PeakAnalysis {
  def takeDerivative(rdd:RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    // central finite difference
    rdd.map(vec =>{
      val output = DenseVector.zeros[Double](vec.size)
      output(1 until vec.size-1) := (vec(2 until vec.size) - vec(0 until vec.size-2)) / DenseVector.fill(vec.size-2){2.0}
      // for the first and last point, because we can't do a central finite difference, do a forward/backwards one instead
      output(0) = vec(1) - vec(0)
      output(vec.size - 1) = vec(vec.size-1) - vec(vec.size-2)
      output
    })
  }

  def findPeak(spark:SparkSession): Unit = {
    import spark.implicits._
    val df = Spark.loadData("Confirmed World")
    val metadata_only = df.select("Province/State", "Country/Region", "Lat", "Long").rdd
    val data_only = df.drop("Province/State", "Country/Region", "Lat", "Long").rdd
      .map(row => DenseVector(row.toSeq.toArray.map(x => x.asInstanceOf[String].toDouble)))
    val daily_cases = takeDerivative(data_only)


    val derivative = takeDerivative(daily_cases)
    val second_derivative = takeDerivative(derivative)

    derivative.take(12).foreach(x => println(x))
    println()
    second_derivative.take(12).foreach(x => println(x))

    val peaks_indices = derivative.zip(second_derivative).map({
      case (deriv1, deriv2) => {
        val zipped = deriv1.toArray.zip(deriv2.toArray).zipWithIndex
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
