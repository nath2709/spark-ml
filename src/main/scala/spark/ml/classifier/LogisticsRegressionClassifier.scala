package spark.ml.classifier

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
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window

object LogisticsRegressionClassifier {

  val spark = SparkSession
    .builder().master("local")
    .appName("news classifier")
    .getOrCreate()

  val newsSchema = Encoders.product[newsfeeds].schema

  val news_class_to_label: String => Int = { s =>

    s.trim match {
      case "Sports"     => 0
      case "Education"  => 1
      case "Technology" => 2
      case "Health"     => 3
      case "Business"   => 4
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
  //to run in widows envirnoment
  System.setProperty("hadoop.home.dir", "D:/winutils")

  def classify(): Unit = {
    //
    val newsData = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data.csv")

    val labelnewsData = newsData.
      withColumn("label", news_class_to_label_udf(newsData("label1"))).
      withColumn("clean_description", removenonalpha(newsData("description")))
      .select("label", "clean_description")

    //    labelnewsData.printSchema()
    val filterData = labelnewsData.filter(labelnewsData("label") !== 10)

    val tokenizer = new Tokenizer().setInputCol("clean_description").setOutputCol("words")
    //    val tokenDf = tokenizer.transform(filterData)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("words_without_stopwords")
    //    val df = remover.transform(tokenDf)

    val hashingTF = new HashingTF()
      .setInputCol("words_without_stopwords").setOutputCol("features").setNumFeatures(20)
    //
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, lr))

    val model = pipeline.fit(filterData)

    model.write.overwrite().save("news_classifier_lr_model")

  }

  def testclassfiermodel(): Unit = {
    import spark.implicits._
    val newsclassifier = PipelineModel.load("news_classifier_lr_model")

    val newsData = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data/op/*").as[newsfeeds]

    val labelnewsData = newsData.
      withColumn("label", news_class_to_label_udf(newsData("label1"))).
      withColumn("clean_description", removenonalpha(newsData("description")))
      .select("label", "clean_description")

    val predictedLableNum = newsclassifier.transform(labelnewsData)
      .select("clean_description", "probability", "prediction")

    predictedLableNum.show()
    //

  }

  def generateTestData(): Unit = {
    import spark.implicits._
    val newsSchema = Encoders.product[newsfeeds].schema
    val newsSchema_idx = Encoders.product[newsfeedsidx].schema

    //    convert to dataset
    val newsDatatest = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data/sample2.txt").as[newsfeeds]
    val newsDatawithId = newsDatatest.rdd.zipWithIndex().map(f => newsfeedsidx(f._1.label1, f._1.description, f._2)).toDS()
    //    newsDatawithId.printSchema()

    //    window function, specify window
    val row_window = Window.partitionBy($"label").orderBy($"idx")
    //row number
    val row_q = row_number().over(row_window)

    //    reduce number of partition from 200(default) to 1
    val temp = newsDatawithId.select($"*", row_q as "row").where($"row" <= 2).repartition(1)
    temp.write.format("csv").option("delimiter", "=").save("data/op")
  }

  def main(args: Array[String]): Unit = {

    //    classify()
    //    generateTestData()
    //    testclassfiermodel()
  }

  case class newsfeeds(label1: String, description: String)
  case class newsfeedsidx(label: String, description: String, idx: Long)
}


