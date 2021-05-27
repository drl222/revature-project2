

object Trends {
  def run(): Unit = {

    val spark = Spark.sparkRun()
    val s = "All Data"
    val d = "Deaths US"
    val Q = "Confirmed US"
    val dw = "Deaths World"
    val cw = "Confirmed World"
    //loading my data in from 3 files
    Spark.loadData(d).createOrReplaceTempView("deathRatio")
    Spark.loadData(Q).createOrReplaceTempView("confirmedRatio")
    Spark.loadData(s).createOrReplaceTempView("people")
    Spark.loadData(dw).createOrReplaceTempView("DeathWorld")
    Spark.loadData(cw).createOrReplaceTempView("ConWorld")

    // start of US based data
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
    val cases2 = spark.sql(" select sum(cast(`5/2/21` as int)) as caseSum from confirmedRatio where Province_State not in ('Arizona', 'California' , 'Illinois', 'Washington' )  ")
    val World = spark.sql("select round(467905 / 26068447 * 100,2) as US_Death_Percent ").show



    // start of world wide data
    val WorldWide = spark.sql("create TEMPORARY view ww as Select * from people where Confirmed > 0")
    val ww2 = spark.sql("create TEMPORARY view ww2 as select * from ( select ObservationDate, `Country/Region`, cast(confirmed as double), deaths, ROW_NUMBER() over (Partition by `Country/Region` order by cast(confirmed as double) ) rn from ww ) ww where rn = 1 order by cast(confirmed as double  )  ")
    val ww3 = spark.sql("create TEMPORARY view ww3 as select ObservationDate, `Country/Region`, confirmed from ww2 order by `Country/Region`")
    val ww4 = spark.sql("create TEMPORARY view ww4 as Select * from ww3 where `Country/Region` not like '%,%' and `Country/Region` not like '%(%'")


    val deathsworld = spark.sql("create TEMPORARY view ww5 as select `Country/Region`, sum(cast(`5/2/21` as int)) as deathSumWorld from DeathWorld group by `Country/Region` order by `Country/Region` ")
    val consworld = spark.sql("create TEMPORARY view ww6 as select `Country/Region`, sum(cast(`5/2/21` as int)) as conSumWorld from ConWorld group by `Country/Region` order by `Country/Region` ")
    val joinagain = spark.sql("create TEMPORARY view ww7 as select f.ObservationDate, d.`Country/Region`,d.deathSumWorld  from ww4 f join ww5 d on (f.`Country/Region` = d.`Country/Region`)")
    val joinagain2 = spark.sql(" create TEMPORARY view ww8 as select d.ObservationDate, d.`Country/Region`,d.deathSumWorld, f.conSumWorld  from ww6 f join ww7 d on (f.`Country/Region` = d.`Country/Region`)")

    val success2 = spark.sql("select *, round(deathSumWorld / conSumWorld * 100,2 ) as Death_Percent  from ww8 where deathSumWorld > 1000 order by Death_percent DESC").show(50)
    spark.close()
  }
}
