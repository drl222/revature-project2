

object Trends {
  def run(): Unit = {

    val spark = Spark.sparkRun()
    val s = "All Data"
    val d = "Deaths US"
    val Q = "Confirmed US"
    //loading my data in from 3 files
    Spark.loadData(d).createOrReplaceTempView("deathRatio")
    Spark.loadData(Q).createOrReplaceTempView("confirmedRatio")
    Spark.loadData(s).createOrReplaceTempView("people")
    val ff = spark.sql("create TEMPORARY view dd as Select * from people where `Country/Region` = 'US' and Confirmed > 0")
    val qq = spark.sql("create TEMPORARY view bb as select * from ( select ObservationDate, `Province/State`, cast(confirmed as double), deaths, ROW_NUMBER() over (Partition by `Province/State` order by cast(confirmed as double) ) rn from dd ) dd where rn = 1 order by cast(confirmed as double  )  ")


    val ww = spark.sql(" create TEMPORARY view test as select ObservationDate, `Province/State`, confirmed from bb where `Province/State` in ('Alaska' , 'Alabama' , 'Arizona' , 'Arkansas' , 'California' , 'Colorado'" +
      " , 'Connecticut' , 'Delaware' , 'Florida' , 'Georgia' , 'Hawaii' , 'Idaho' , 'Illinois'" +
      " , 'Texas' , 'Indiana' , 'Iowa' , 'Kansas' , 'Kentucky' , 'Louisiana' , 'Maine'" +
      " , 'Maryland' , 'Massachusetts' , 'Michigan' , 'Minnesota' , 'Mississippi' , 'Missouri' , 'Montana'" +
      " , 'Nebraska' , 'Nevada' , 'New Hampshire' , 'New Jersey' , 'New York' , 'North Carolina' , 'North Dakota' , 'Ohio'" +
      " , 'Oklahoma' , 'Oregon' , 'Pennsylvania' , 'Rhode Island' , 'South Carolina' , 'South Dakota'" +
      ", 'Tennessee' , 'Utah' , 'Vermont' , 'Virginia' , 'Washington' , 'West Virginia' , 'Wisconsin' , 'Wyoming') order by `Province/State` ")

    val frist_case = spark.sql("create TEMPORARY view first as select ObservationDate, `Province/State`, confirmed from test where ObservationDate like '%01%'")

    val deaths = spark.sql("create TEMPORARY view deaths as select Province_State, sum(cast(`5/2/21` as int)) as deathSum from deathRatio where Province_State in ('Arizona'," +
      " 'California' , 'Illinois', 'Washington' )group by Province_State order by Province_State    ")

    val cases = spark.sql("create TEMPORARY view cases as select Province_State, sum(cast(`5/2/21` as int)) as caseSum from confirmedRatio where Province_State in ('Arizona'," +
      " 'California' , 'Illinois', 'Washington' )group by Province_State order by Province_State ")

    val join_one = spark.sql("create TEMPORARY view joiner as select f.ObservationDate, d.Province_State,d.deathSum  from first f join deaths d on (f.`Province/State` = d.Province_State)")


    val join_two = spark.sql("create TEMPORARY view ratio as select f.ObservationDate, f.Province_State, f.deathSum, c.caseSum from joiner f join cases c on (f.Province_State = c.Province_State)")
    val success = spark.sql("select *, round(deathSum / caseSum * 100,2 ) as Death_Percent  from ratio ").show


    val deaths2 = spark.sql(" select sum(cast(`5/2/21` as int)) as deathSum from deathRatio where Province_State not in ('Arizona'," +
      " 'California' , 'Illinois', 'Washington' )    ")

    val cases2 = spark.sql(" select sum(cast(`5/2/21` as int)) as caseSum from confirmedRatio where Province_State not in ('Arizona', 'California' , 'Illinois', 'Washington' )  " )

    val World = spark.sql("select round(467905 / 26068447 * 100,2) as US_Death_Percent ").show

    spark.close()
  }

}
