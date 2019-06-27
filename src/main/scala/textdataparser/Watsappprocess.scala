package textdataparser

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import java.io.FileWriter
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import java.util.Formatter.DateTime
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType

object Watsappprocess {

  case class detail(date: String, name: String, msg: String)

  def processfile(filename: String): Unit = {

    val dateRegex = "\\s+\\W+\\s".r
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    val spark = SparkSession.builder().appName("watsappprocess").master("local[*]").getOrCreate()
    val raw_data = spark.read.text(filename)

    //    raw_data.show(false)

    import spark.implicits._
    val schema = Encoders.product[detail]

    val rows = raw_data.map(x => {
      //      println(x.mkString(""))

      val y = x.getString(0).split(dateRegex.toString())

      val temp = y(1).indexOf(":")
      val (name, msg) = temp match {
        case -1   => ("", y(1))
        case temp => (y(1).substring(0, temp), y(1).substring(temp+1, y(1).length()))
      }

      detail(y(0), name, msg)
    })

    val rowDS = rows.as(schema).select(to_timestamp($"date", "MM/dd/yy, HH:mm").as("timestamp"), $"name", $"msg")
    process_msg(rowDS, "Abhishek Singhvi")

    //    val msg_name = rowDS.withColumn("mon", month($"timestamp")).withColumn("year", year($"timestamp"))
    //    //    val msg_name_filter = msg_name.filter($"mon" === null)
    //    //    msg_name.show(10000)
    //    val msg_mon_grpby = msg_name.groupBy($"year", $"mon", $"name").agg(count($"*").alias("count"))
    //
    //    val windowspec = Window.partitionBy(col("year"), col("mon")).orderBy(col("count").desc)
    //    val msg_dense_rank = dense_rank().over(windowspec)
    //    msg_mon_grpby.select($"year", $"mon", $"name", $"count", msg_dense_rank.alias("rank")).show(240)
    //

    //    rowDS.sel
    //    val op = rowDS.groupBy(year($"timestamp"),month($"timestamp"),$"name").count().sort($"count".desc).sort($"month(timestamp)".desc)

    //    raw_data.map(func, encoder)
    //    op.show(false)

  }

  def process_msg(ds: Dataset[Row], name: String): Unit = {

    val stopwords = Source.fromFile("data/stop-word-list.txt").getLines().toArray

    def to_string(temp: TraversableOnce[String]) = {

      temp.mkString("").replaceAll(":","")
      
    }

    val to_string_udf = udf(to_string(_: TraversableOnce[String]))

    val msg_ds = ds.select("*").where(col("name") === name).groupBy("name").agg(collect_list("msg").alias("msgs")).withColumn("temp_msgs", to_string_udf(col("msgs")))

//    msg_ds.show(false)

    val tokenizer = new Tokenizer().setInputCol("temp_msgs").setOutputCol("tokens")
    val stopwordstransformer = new StopWordsRemover().setStopWords(stopwords).setInputCol("tokens").setOutputCol("tokens_rm")

    val pipeline = new Pipeline().setStages(Array(tokenizer, stopwordstransformer))
    val model = pipeline.fit(msg_ds)

    val trans_model = model.transform(msg_ds).select("name","tokens_rm")
    val trans_model_ex = trans_model.select(col("name"), explode(col("tokens_rm")).as("words"))
    
    val temp = trans_model_ex.groupBy("name", "words").agg(count("*").alias("count")).sort(col("count").desc)
//    temp.select("name", "words","count").where(col("words").like("saa*")).show
    temp.show(100)
    //    //   val op = trans_model.groupBy("name").agg(collect_list("tokens_rm"))
//        trans_model_ex.show()
    //   op.foreach(x => println(x.get(1)))

  }

  def filepreprocessor(filename: String): Unit = {
    val lines = Source.fromFile(filename).getLines().toList

    val x = parse(lines, "")
    val t = "hello , this is example"
    val z = x.split("&&&")
    val writer = new FileWriter("D:/Data/WhatsAppChat_formatted.txt")
    writer.write(z.mkString("\n"))

    writer.flush()
    writer.close()
  }

  def parse(lines: List[String], temp: String): String = {

    val dateRegex = "\\d+\\W\\d+\\W\\d{2}\\W+\\d+\\W\\d+".r

    //    println(temp,lines)
    println(lines.size)
    lines match {
      case Nil => {
        println("finish")
        return temp
      }
      case x :: line => {

        val y = dateRegex.findFirstIn(x).getOrElse("")
        var temp1 = ""
        if (y.equals("")) {
          temp1 = temp.concat(" $#$# " + x)
          parse(line, temp1)
        } else {
          parse(line, temp.concat("&&&" + x))
        }

      }
    }

  }

  def main(args: Array[String]): Unit = {
    //    filepreprocessor("D://Data//WhatsAppChat.txt")
    processfile("D://Data//WhatsAppChat_formatted.txt")

  }

}

