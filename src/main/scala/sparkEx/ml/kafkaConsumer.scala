package sparkEx.ml

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import scala.collection.mutable.Queue
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel

object kafkaConsumer {

  val toCategory: Double => String = { s =>

    s match {
      case 0.0 => "Health"
      case 1.0 => "education"
      case 2.0 => "tech"
      case 3.0 => "health"
      case 4.0 => "business"

    }
  }

  val customFunct = udf(toCategory)

  def posttokafka(): Unit = {

    System.setProperty("hadoop.home.dir", "D:/winutils")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount").master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val userSchema = new StructType().add("value", "string").add("description", "string")
    val csvDF = spark
      .readStream
      .option("sep", "=").option("fileNameOnly", true)
      .schema(userSchema) // Specify schema of the csv files
      .csv("file:///D:/scala-eclipse/sparkEx/data/*")

    val temp = csvDF.select("description")
    val nonempty = temp.as[String].filter(_ != "").map(_.toLowerCase())
    //    temp.show()
    val words = nonempty.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val tmp = wordCounts.select((to_json(struct("count", "value"))).alias("value"))
    tmp.printSchema()

    //    val test = testData
    //
    //    // Make predictions on test documents.

    val sameModel = PipelineModel.load("spark-logistic-regression-model")

    val predictedLableNum = sameModel.transform(temp)
      .select("description", "probability", "prediction")

    val labledData = predictedLableNum.withColumn("category", customFunct(predictedLableNum("prediction"))).select("description", "probability", "prediction", "category")
    val labledDataJson = labledData.select((to_json(struct("description", "category"))).alias("value"))

    val query = labledDataJson
      .writeStream.outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "test").option("checkpointLocation", "chk")
      .start()

    //    val query = labledDataJson.writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    posttokafka()
  }
}