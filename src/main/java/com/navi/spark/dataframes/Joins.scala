package com.navi.spark.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Joins extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF =  spark.read.json("src/main/resources/guitars.json")
  val guitarPlayersDF =  spark.read.json("src/main/resources/guitar_players.json")
  val bandsDF =  spark.read.json("src/main/resources/bands.json")

  guitarsDF.show()
  guitarPlayersDF.show()
  bandsDF.show()

  //inner
  val guitaristsBandsDF = guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"))
  guitaristsBandsDF.show(false)

  //left outer join
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "left_outer").show()

  //right outer join
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "right_outer").show()

  //full
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "full_outer").show()

  //semi joins
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "left_semi").show()
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "left_anti").show()

  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitarPlayersDF.join(bandsModDF, guitarPlayersDF.col("band") === bandsModDF.col("bandId"))

  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarID"), expr("array_contains(guitars,guitarID)")).show()

  val readTable = (table: String) => spark.read.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
    .option("user", "root").option("password", "password").option("dbtable", s"test.$table").load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

  // 2
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )
  empNeverManagersDF.show()

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()

}
