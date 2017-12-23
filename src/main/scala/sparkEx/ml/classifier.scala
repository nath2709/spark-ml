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

    val newsData = spark.read.format("csv").option("delimiter", "=").schema(newsSchema).load("data.csv")

    //    val newCol = when(col("C").equalTo("A"), "X")
    //    .when(col("C").equalTo("B"), "Y")
    //    .otherwise("Z");
    //    people.select(when(people("gender") === "male", 0)
    //      .when(people("gender") === "female", 1)
    //      .otherwise(2))
    //    val newCol = newsData.select(when(newsData("label") === "sports", 0)
    //     .when(newsData("label") === "tech", 1)
    //     .otherwise(2)).col("label")

    //    val newCol = when(newsData("label") === "education", 1)
    //      .when(newsData("label") === "business", 2)
    //      .when(newsData("label") === "tech", 3)
    //      .when(newsData("label") === "health", 4)
    //    val newCol = newsData.col("label")

    val labelnewsData = newsData.withColumn("label", customFunct(newsData("label1"))).select("label", "description")
    val filterData = labelnewsData.filter(labelnewsData("label") !== 10)
    //    filterData.show(1000)
    //      .when(newsData("label") === "education", 1)
    //      .when(newsData("label") === "business", 2)
    //      .when(newsData("label") === "tech", 3)
    //      .when(newsData("label") === "health", 4))
    //    labelnewsData.show(100)

    //    val sentenceData = spark.createDataFrame(Seq(
    //      (0.0, "Hi I heard about Spark"),
    //      (0.0, "I wish Java could use case classes"),
    //      (1.0, "Logistic regression models are neat"))).toDF("label", "sentence")

    //    val toUpper: String => String = _.toUpperCase
    //    val upper = udf(toUpper)
    //    sentenceData.withColumn("upper", upper(sentenceData("sentence"))).show
    //
    val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("words")
    //    val wordsData = tokenizer.transform(filterData)

    //    wordsData.show()
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("features").setNumFeatures(20)

    //    hashingTF.transform(filterData).show
    //      
    //        val featurizedData = hashingTF.transform(wordsData)
    //    //    // alternatively, CountVectorizer can also be used to get term frequency vectors
    //    //
    //        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //    val idfModel = idf.fit(featurizedData)
    //
    //    val rescaledData = idfModel.transform(featurizedData)
    //    rescaledData.select("label", "features").show()

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(filterData)

    // Now we can optionally save the fitted pipeline to disk
    //    model.write.overwrite().save("spark-logistic-regression-model")
    //
    //    // We can also save this unfit pipeline to disk
    //    pipeline.write.overwrite().save("unfit-lr-model")

    // And load it back in during production
    //    val sameModel = PipelineModel.load("spark-logistic-regression-model")

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (1, "spark i j k"),
      (2, "l m n"),
      (4, "spark hadoop spark"),
      (0, "apache hadoop"))).toDF("label", "description")

    //    val test = testData
    //
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