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
import org.apache.spark.ml.feature.StopWordsRemover

object classifier {

  val spark = SparkSession
    .builder().master("local")
    .appName("news classifier")
    .getOrCreate()

  val newsSchema = Encoders.product[newsfeeds].schema

  val news_class_to_label: String => Int = { s =>

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

  val remove_num_splchars: String => String = { data =>

    val regex = "[^a-z\\s]"

    data.toLowerCase().replaceAll(regex, "")

  }

  val op = remove_num_splchars("Tickets worth Rupees 52,36,000 were unsold.")
  val news_class_to_label_udf = udf(news_class_to_label)
  val removenonalpha = udf(remove_num_splchars)
  //
  def classify(): Unit = {
    //    //    System.setProperty("hadoop.home.dir", "D:/winutils")
    //
    val newsData = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data.csv")

    val labelnewsData = newsData.
      withColumn("label", news_class_to_label_udf(newsData("label1"))).
      withColumn("clean_description", removenonalpha(newsData("description")))
      .select("label", "clean_description")

//    labelnewsData.printSchema()
    val filterData = labelnewsData.filter(labelnewsData("label") !== 10)

    val tokenizer = new Tokenizer().setInputCol("clean_description").setOutputCol("words")
    val tokenDf = tokenizer.transform(filterData)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("words_without_stopwords")
    val df = remover.transform(tokenDf)
    //
    df.show(false)

    //    val hashingTF = new HashingTF()
    //      .setInputCol("words").setOutputCol("features").setNumFeatures(20)
    //
    //    val lr = new LogisticRegression()
    //      .setMaxIter(10)
    //      .setRegParam(0.001)
    //
    //    val pipeline = new Pipeline()
    //      .setStages(Array(tokenizer, hashingTF, lr))
    //
    //    val model = pipeline.fit(filterData)
    //
    //    model.write.overwrite().save("spark-logistic-regression-model")
    //
    //    val test = spark.createDataFrame(Seq(
    //      (1, "spark i j k"),
    //      (2, "l m n"),
    //      (4, "spark hadoop spark"),
    //      (0, "apache hadoop"))).toDF("label", "description")
    //
    //    //    // Make predictions on test documents.
    //    model.transform(test)
    //      .select("label", "description", "probability", "prediction")
    //      .collect()
    //      .foreach {
    //        println
    //        //        case Row(label: Long, sentence: String, prob: Vector, prediction: Double) =>
    //        //          println(s"($label, $sentence) --> prob=$prob, prediction=$prediction")
    //      }
  }

  def testData(): DataFrame = {
    val newsDatatest = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data2.csv")
    val labelnewsDatatest = newsDatatest.withColumn("label", news_class_to_label_udf(newsDatatest("label1"))).select("label", "description")

    labelnewsDatatest.select("description")
  }

  def main(args: Array[String]): Unit = {

    //   testData()
    classify()
  }
}

case class newsfeeds(label1: String, description: String)