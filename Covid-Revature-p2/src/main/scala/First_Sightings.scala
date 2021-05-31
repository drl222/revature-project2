import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object First_Sightings {
  val s = "All Data"
  val d = "Deaths US"
  val Q = "Confirmed US"
  val dw = "Deaths World"
  val cw = "Confirmed World"
  Spark.loadData(d).createOrReplaceTempView("deathRatio")
  Spark.loadData(Q).createOrReplaceTempView("confirmedRatio")
  Spark.loadData(s).createOrReplaceTempView("people")
  Spark.loadData(dw).createOrReplaceTempView("DeathWorld")
  Spark.loadData(cw).createOrReplaceTempView("ConWorld")

  //loading my data in from 3 files

  def US_stats(spark: SparkSession): Unit = {

    // start of US based data
     spark.sql("create or replace TEMPORARY  view dd as Select * from people where `Country/Region` = 'US' and Confirmed > 0")
     spark.sql("create or replace TEMPORARY view bb as select * from ( select ObservationDate, `Province/State`, cast(confirmed as double), deaths, ROW_NUMBER() over (Partition by `Province/State` order by cast(confirmed as double) ) rn from dd ) dd where rn = 1 order by cast(confirmed as double  )  ")


    val ww = spark.sql(" create or replace TEMPORARY view test as select ObservationDate, `Province/State`, confirmed from bb where `Province/State` in ('Alaska' , 'Alabama' , 'Arizona' , 'Arkansas' , 'California' , 'Colorado'" +
      " , 'Connecticut' , 'Delaware' , 'Florida' , 'Georgia' , 'Hawaii' , 'Idaho' , 'Illinois'" +
      " , 'Texas' , 'Indiana' , 'Iowa' , 'Kansas' , 'Kentucky' , 'Louisiana' , 'Maine'" +
      " , 'Maryland' , 'Massachusetts' , 'Michigan' , 'Minnesota' , 'Mississippi' , 'Missouri' , 'Montana'" +
      " , 'Nebraska' , 'Nevada' , 'New Hampshire' , 'New Jersey' , 'New York' , 'North Carolina' , 'North Dakota' , 'Ohio'" +
      " , 'Oklahoma' , 'Oregon' , 'Pennsylvania' , 'Rhode Island' , 'South Carolina' , 'South Dakota'" +
      ", 'Tennessee' , 'Utah' , 'Vermont' , 'Virginia' , 'Washington' , 'West Virginia' , 'Wisconsin' , 'Wyoming') order by `Province/State` ")

     spark.sql("create or replace TEMPORARY view first as select ObservationDate, `Province/State`, confirmed from test where ObservationDate like '%01%'")
     spark.sql("create or replace TEMPORARY view deaths as select Province_State, sum(cast(`5/2/21` as int)) as deathSum from deathRatio where Province_State in ('Arizona'," +
      " 'California' , 'Illinois', 'Washington' )group by Province_State order by Province_State    ")

    val cases = spark.sql("create or replace TEMPORARY view cases as select Province_State, sum(cast(`5/2/21` as int)) as caseSum from confirmedRatio where Province_State in ('Arizona'," +
      " 'California' , 'Illinois', 'Washington' )group by Province_State order by Province_State ")

    spark.sql("create or replace TEMPORARY view joiner as select f.ObservationDate, d.Province_State,d.deathSum  from first f join deaths d on (f.`Province/State` = d.Province_State)")
    spark.sql("create or replace TEMPORARY view ratio as select f.ObservationDate, f.Province_State, f.deathSum, c.caseSum from joiner f join cases c on (f.Province_State = c.Province_State)")
    spark.sql("select *, round(deathSum / caseSum * 100,2 ) as Death_Percent  from ratio ").show

    spark.sql(" select sum(cast(`5/2/21` as int)) as deathSum from deathRatio where Province_State not in ('Arizona'," +
      " 'California' , 'Illinois', 'Washington' )    ")
     spark.sql(" select sum(cast(`5/2/21` as int)) as caseSum from confirmedRatio where Province_State not in ('Arizona', 'California' , 'Illinois', 'Washington' )  ")
     spark.sql("select round(467905 / 26068447 * 100,2) as US_Death_Percent ").show

  }


  def World_Stats(spark: SparkSession): Unit = {
    // start of world wide data
    spark.sql("create or replace TEMPORARY view ww as Select * from people where Confirmed > 0")
    spark.sql("create or replace TEMPORARY view ww2 as select * from ( select ObservationDate, `Country/Region`, cast(confirmed as double), deaths, ROW_NUMBER() over (Partition by `Country/Region` order by cast(confirmed as double) ) rn from ww ) ww where rn = 1 order by cast(confirmed as double  )  ")
    spark.sql("create or replace TEMPORARY view ww3 as select ObservationDate, `Country/Region`, confirmed from ww2 order by `Country/Region`")
    spark.sql("create or replace TEMPORARY view ww4 as Select * from ww3 where `Country/Region` not like '%,%' and `Country/Region` not like '%(%'")


    spark.sql("create or replace TEMPORARY view ww5 as select `Country/Region`, sum(cast(`5/2/21` as int)) as deathSumWorld from DeathWorld group by `Country/Region` order by `Country/Region` ")
    spark.sql("create or replace TEMPORARY view ww6 as select `Country/Region`, sum(cast(`5/2/21` as int)) as conSumWorld from ConWorld group by `Country/Region` order by `Country/Region` ")
    spark.sql("create or replace TEMPORARY view ww7 as select f.ObservationDate, d.`Country/Region`,d.deathSumWorld  from ww4 f join ww5 d on (f.`Country/Region` = d.`Country/Region`)")
    spark.sql(" create or replace TEMPORARY view ww8 as select d.ObservationDate, d.`Country/Region`,d.deathSumWorld as TotalDeaths, f.conSumWorld as TotalCases  from ww6 f join ww7 d on (f.`Country/Region` = d.`Country/Region`)")

    spark.sql(" create or replace TEMPORARY view ww9 as select *, round(TotalDeaths / TotalCases * 100,2 ) as Death_Percent  from ww8 where TotalDeaths > 1000 order by Death_percent DESC")
    val result = spark.sql("select * from ww9")

    result.coalesce(1).write.format("csv").option("header", true).mode(SaveMode.Overwrite).save("hdfs://localhost:9000/user/project2/First_Sighting_output.csv")
    result.show()


    spark.sql("create or replace TEMPORARY view  jan1 as select * from ww9 where ObservationDate like '01%' ")
    spark.sql("create or replace TEMPORARY view  feb1 as select * from ww9 where ObservationDate like '02%' ")
    spark.sql(" create or replace TEMPORARY view march1 as select * from ww9 where ObservationDate like '03%' ")


    val results2 = spark.sql("select 'January' as MonthOfFirstOccurrence, Round(avg(Death_Percent),2) as DeathPercentageByMonth from jan1  Union " +
      "select 'February' as MonthOfFirstOccurrence, Round(avg(Death_Percent),2) as DeathPercentageByMonth from feb1 union " +
      "select 'March' as MonthOfFirstOccurrence, Round(avg(Death_Percent),2) as DeathPercentageByMonth from march1")
    results2.coalesce(1).write.format("csv").option("header", true).mode(SaveMode.Overwrite).save("hdfs://localhost:9000/user/project2/First_Sighting_Breakdown_output.csv")
    results2.show()
  }

}
