import scala.io.StdIn

object CLI
{


  def menu(): Unit = {
    val spark = Spark.sparkRun()
    println("Please choose one of the following options")
    var loopcon = true

    while (loopcon == true) {


      printmenu()
      var input = StdIn.readInt()

      input match {
        case 1 => {
          Trends.US_stats(spark)

        }

        case 2 => {
          Trends.World_Stats(spark)

        }

        case 3 => {

          PeakAnalysis.findPeakV2(spark)

        }

        case 4 => {
          Geographic.geographicalData(spark)
        }

        case 5 => {

          // exits the program
          spark.close()
          println("Exiting...")
          loopcon = false

        }


        case _ => {
          //error checking
          println("Please choose a correct menu option")
        }
      }

    }

  }


      //names can be changed. this is just a placeholder
      def printmenu(): Unit = {
      List("Menu Options", "1. USA Stats", "2. World Stats", "3. Peak Stats", "4. Geo Stats", "5. Exit").foreach(println)
    }



}
