package sparkEx.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ DataFrame }

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.functions.udf

object classifier {

  val spark = SparkSession
    .builder().master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()

  val newsSchema = Encoders.product[newsfeeds].schema

  val toUpper: String => Int = { s =>

    s.trim match {
      case "sports"    => 0
      case "education" => 1
      case "tech"      => 2
      case "health"    => 3
      case "business"  => 4
      case _ => {
        10
      }

    }
  }

  val customFunct = udf(toUpper)

  def classify(): Unit = {
    //    System.setProperty("hadoop.home.dir", "D:/winutils")

    val newsData = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data.csv")

    val labelnewsData = newsData.withColumn("label", customFunct(newsData("label1"))).select("label", "description")
    val filterData = labelnewsData.filter(labelnewsData("label") !== 10)

    val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("words")

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("features").setNumFeatures(20)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(filterData)

    model.write.overwrite().save("spark-logistic-regression-model")

    val test = spark.createDataFrame(Seq(
      (1, "spark i j k"),
      (2, "l m n"),
      (4, "spark hadoop spark"),
      (0, "apache hadoop"))).toDF("label", "description")

    //    // Make predictions on test documents.
    model.transform(test)
      .select("label", "description", "probability", "prediction")
      .collect()
      .foreach {
        println
        //        case Row(label: Long, sentence: String, prob: Vector, prediction: Double) =>
        //          println(s"($label, $sentence) --> prob=$prob, prediction=$prediction")
      }
  }

  def testData(): DataFrame = {
    val newsDatatest = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data2.csv")
    val labelnewsDatatest = newsDatatest.withColumn("label", customFunct(newsDatatest("label1"))).select("label", "description")

    labelnewsDatatest.select("description")
  }

  def main(args: Array[String]): Unit = {

    //   testData()
    classify()
  }
}

case class newsfeeds(label1: String, description: String)