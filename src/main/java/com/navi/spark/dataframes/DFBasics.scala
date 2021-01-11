package com.navi.spark.dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.collection.mutable

object DFBasics extends App{
  val spark = SparkSession.builder()
    .appName("dfBasics")
    .config("spark.master","local")
    .getOrCreate()
  val firstDF = spark.read.format("json")
    .option("inferschema", "true")
    .load("src/main/resources/cars.json")
  firstDF.printSchema()
  firstDF.show(10)

  firstDF.tail(10) foreach println
  //spark types
  val longType = LongType
  val doubleType = DoubleType

  val customType = StructType(
    Array(
      StructField("f1", StringType, true),
      StructField("f2", LongType)
    )
  )


  val dfWithInvalidSchema = spark.read.format("json")
    .schema(customType)
    .load("src/main/resources/cars.json")
  /**
   * +----+----+
   * |f1  |f2  |
   * +----+----+
   * |null|null|
   * |null|null|
   * |null|null|
   * |null|null|
   * |null|null|
   * +----+----+
   * only showing top 5 rows
   */
  dfWithInvalidSchema.show(5,false)
  val s = StructType(
    Array(StructField("Acceleration",DoubleType,true), StructField("Cylinders",LongType,true), StructField("Displacement",DoubleType,true), StructField("Horsepower",LongType,true), StructField("Miles_per_Gallon",DoubleType,true), StructField("Name",StringType,true), StructField("Origin",StringType,true), StructField("Weight_in_lbs",LongType,true), StructField("Year",StringType,true)))
  println("Schema " + s)

  val dfWithSchema = spark.read.format("json")
    .schema(s)
    .option("mode", "failFast")
    .load("src/main/resources/cars.json")
  /**
   * +------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
   * |Acceleration|Cylinders|Displacement|Horsepower|Miles_per_Gallon|Name                     |Origin|Weight_in_lbs|Year      |
   * +------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
   * |12.0        |8        |307.0       |130       |18.0            |chevrolet chevelle malibu|USA   |3504         |1970-01-01|
   * |11.5        |8        |350.0       |165       |15.0            |buick skylark 320        |USA   |3693         |1970-01-01|
   * |11.0        |8        |318.0       |150       |18.0            |plymouth satellite       |USA   |3436         |1970-01-01|
   * |12.0        |8        |304.0       |150       |16.0            |amc rebel sst            |USA   |3433         |1970-01-01|
   * |10.5        |8        |302.0       |140       |17.0            |ford torino              |USA   |3449         |1970-01-01|
   * +------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
   */
  dfWithSchema.show( false)

  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA"))

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred
  manualCarsDF.printSchema()
  //DFs have schema, rows dont

  //create df with implicits
  import spark.implicits._
  val implicitDF = cars.toDF()

  implicitDF.show(5, false)
  implicitDF.printSchema()

  //load from and write to DB
  val dfFromDB = spark.read.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
    .option("user", "root").option("password", "password").option("dbtable", "test.person").load()
  dfFromDB.show(false)

  firstDF.write.format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false")
    .option("user", "root").option("password", "password").option("dbtable", "test.cars").save()

  /*case class LBS(lat:Double, long:Double, id: String)
  case class POI(lat:Double, long:Double, locId:String)
  val lbs = Seq(LBS(1.0, 2.5, "c1"), LBS(4.8, 1.3, "c2"),LBS(4.7, 1.2, "c3"),LBS(1.1, 2.3, "c4"))
  val poi = Seq(POI(1.2, 2.4, "m1"), POI(4.6, 1.1, "u1"), POI(3.5, 2.0, "SG1"))

  val res = mutable.Map.empty[String, String]
  lbs.foreach( l => {
    var minDist = Double.PositiveInfinity;
    var nearestDist = ""
    poi.foreach( p => {
      val d = calculateDist(l.lat,l.long, p.lat,p.long)
      if(minDist > d){
       nearestDist = p.locId
        minDist = d
      }
    })
    res += (l.id -> nearestDist)
  })

  println(res)

  def calculateDist(d: Double, d1: Double, d2: Double, d3: Double) ={
    Math.sqrt(Math.pow(d - d2, 2) +  Math.pow(d1 - d3, 2))
  }*/

}
