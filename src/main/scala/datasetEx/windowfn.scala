
package datasetEx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object windowfn {

  def windowfnex(): Unit = {
    val spark = SparkSession.builder().appName("window_fn_example").
      master("local").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
    import spark.implicits._

    val schema = Encoders.product[employee].schema
    val employee = spark.read.option("header", "True").schema(schema).csv("data/employee.csv").as[employee]

    val exp = Window.partitionBy($"department").orderBy($"salary" desc)
    //    rank query
    //    val rank_q = rank().over(exp)
    //    val op = employee.select($"*",rank_q as "rank").show

    //    dense rank query
    val dense_q = dense_rank().over(exp)
    employee.select($"*", dense_q as "rank").show

    //   percent_rank
    //     val percent_q = percent_rank().over(exp)
    //     employee.select($"*",percent_q as "percent_rank").show

    //     row number
    //     val row_num = row_number().over(exp)
    //     employee.select($"*",row_num as "row_num").where($"department"==="Marketing").show

    //    sum
    //      val sum_dept = sum($"salary").over(exp)
    //      employee.select($"*",sum_dept as "total_sum").show

    //    moving average
    //     val mv_window_spec = Window.orderBy($"id").rowsBetween(-1, 1)
    //     val mv_df = employee.withColumn("moving average", avg($"salary").over(mv_window_spec))
    //     mv_df.show()

  }

  def main(args: Array[String]): Unit = {
    windowfnex()

  }
  case class employee(id: Int, first_name: String, last_name: String, department: String, salary: Int, gender: String)

}
