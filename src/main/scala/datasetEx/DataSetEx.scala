package datasetEx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

object DataSetEx {

  val spark = SparkSession.builder().appName("incrload").master("local").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    appendDF()
//    loadtemptable()
    updateDF()
  }

  def appendDF(): Unit = {

    val schema = Encoders.product[person].schema
    val personDs = spark.read.option("header", "true").schema(schema).csv("data/person_data.csv").as[person]
    //    personDs.foreach(p => println(p))
    //     personDs.printSchema()
    personDs.createOrReplaceTempView("person")
  }

  def loadtemptable(): Unit = {

    val persondf = spark.sql("select * from person").as[person]
    val tempRecord = Seq(person(10001, "Allen", "Andrivot", "aandrivot0@mtv.com", "Male", "138.162.66.69")).toDF().as[person]

    val personDfappend = persondf.union(tempRecord)

    personDfappend.foreach(df => println(df))

  }

  def updateDF(): Unit = {
    val persondf = spark.sql("select * from person").as[person]
    val tempRecord = Seq(person(1, "new_Allen", "Andrivot", "aandrivot0@mtv.com", "Male", "138.162.66.69")
        ,person(2,"Liuka","Labrenz_new","llabrenz1@nyu.edu","Female","92.237.104.237")).toDF().as[person]

    val personDfappend = persondf.union(tempRecord)

    personDfappend.foreach(df => println(df))
  }

}

case class person(id: Int, first_name: String, last_name: String, email: String, gender: String, ip_address: String)