import scala.io.StdIn

object CLI {


  def menu(): Unit = {
    val spark = Spark.sparkRun()
    println("Please choose one of the following options")
    var loopcon = true

    while (loopcon == true) {


      printmenu()
      var input = StdIn.readInt()

      input match {


        case 1 => {
          First_Sightings.World_Stats(spark)

        }

        case 2 => {

          PeakAnalysis.findPeakV2(spark)

        }

        case 3 => {
          Geographic.geographicalData(spark)
        }

        case 4 => {

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
    List("Menu Options",  "1. First Sightings Stats", "2. Peak Stats", "3. Geographical Stats", "4. Exit").foreach(println)
  }


}
