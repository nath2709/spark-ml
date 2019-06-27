package ml.dataduplication

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinHashLSHModel
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

object DataDedup {

/***
 * simple spark streaming application to detect near duplicates from unstructured datasets
 *   
 */

  def process_streamingData(): Unit = {

    //  to run on windows

    val spark = SparkSession
      .builder
      .appName("streaming-app").master("local[*]")
      .getOrCreate()

    //    val schema = new StructType().add("value", "string").add("clean_description", "string")
    val schema = Encoders.product[newstime].schema
    import spark.implicits._

    val ds = spark.readStream.option("sep", "$").option("fileNameOnly", true).schema(schema).csv("sample1/*").as[newstime]
    val description = ds.select($"description")
    val mdl_ds = spark.read.option("sep", "$").option("fileNameOnly", true).schema(schema).csv("sample/*").as[newstime]
    val mdl_desc = mdl_ds.select($"description")
    //    mdl_desc.show()

    //    build a pipeline
    val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("tokens")
    val stopwordstransformer = new StopWordsRemover().setInputCol("tokens").setOutputCol("tokens_rm")
    val hashigTf = new HashingTF().setInputCol("tokens_rm").setOutputCol("hashingTf")

    val minhash = new MinHashLSH().setInputCol("hashingTf").setOutputCol("minhash")

    val pipeline = new Pipeline().setStages(Array(tokenizer, stopwordstransformer, hashigTf, minhash))

    val model = pipeline.fit(mdl_desc)

    val result = model.transform(description)
    val temp_mdl = model.transform(mdl_desc)
    //    temp_mdl.show()

    val temp = model.stages.last.asInstanceOf[MinHashLSHModel].approxSimilarityJoin(result, temp_mdl, 0.6)

    val query = temp.writeStream.outputMode("append").format("console").option("truncate", false).start()
    //    val query = temp.writeStream.outputMode("append").format("csv").option("path", "op").option("topic", "test").option("checkpointLocation", "chk").start()
    //
    query.awaitTermination()

  }

  def process(): Unit = {

    //  to run on windows
    System.setProperty("hadoop.home.dir", "D:/winutils")

    val spark = SparkSession
      .builder
      .appName("dedup-app").master("local[*]")
      .getOrCreate()

    val stringToArry: String => Array[String] = { s => s.split(" ") }
    val fn = udf(stringToArry)
    val schema = Encoders.product[newstime].schema
    import spark.implicits._

    val orgds = spark.read.option("sep", "$").option("fileNameOnly", true).schema(schema).csv("sample/*").as[newstime]

    val dupds = spark.read.option("sep", "$").option("fileNameOnly", true).schema(schema).csv("sample1/*").as[newstime]

    val orgdescription = orgds.select($"description")
    val dupdescription = dupds.select($"description")

    val orgdescwords = orgdescription.map(row => row.getAs[String]("description").split(" ")).map(Tuple1.apply).toDF("description")
    val dupdeswords = dupdescription.map(row => row.getAs[String]("description").split(" ")).map(Tuple1.apply).toDF("description")

    //    orgdescwords.show()
    //        temp.show()
    val word2Vec = new Word2Vec()
      .setInputCol("description")
      .setOutputCol("result")
      .setVectorSize(10).setSeed(100)
      .setMinCount(0)
    var model = word2Vec.fit(orgdescwords)
    //
    val orgmdl = model.transform(orgdescwords)
    model = word2Vec.fit(dupdeswords)
    val dupmdl = model.transform(dupdeswords)

    //    orgmdl.show()
    //    dupmdl.show()
    //

    val minhash = new MinHashLSH().setNumHashTables(3).setInputCol("result").setOutputCol("hashes")
    val orgminhashmdl = minhash.fit(orgmdl)
    //    //    //
    val orghasheddata = orgminhashmdl.transform(orgmdl)
    //    orghasheddata.show()

    ////    val test_result = lshmodel.transform(test_r)
    //    //    train_result.show()
    orgminhashmdl.approxSimilarityJoin(orgmdl, dupmdl, 0.001, "JaccardDistance")
      .select(
        col("datasetA.description").alias("idA"),
        col("datasetB.description").alias("idB"),
        col("JaccardDistance"))
      .show(50)

  }

  def process_1(): Unit = {

    System.setProperty("hadoop.home.dir", "D:/winutils")

    //    create spark session
    val spark = SparkSession
      .builder
      .appName("dedup-app").master("local[*]")
      .getOrCreate()

    //      document 1

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat"))).toDF("label", "sentence")

    val sentenceData1 = spark.createDataFrame(Seq(
      (0.0, "hi i heard about spark"),
      (0.0, "I wish if scala could be use jvm"),
      (1.0, "machine learning models are neat "))).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val wordsData1 = tokenizer.transform(sentenceData1)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    val featurizedData1 = hashingTF.transform(wordsData1)

    featurizedData.show(false)
    //    //    Minhash transformer
    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("rawFeatures")
      .setOutputCol("hashes")
    //
    val model_mh = mh.fit(featurizedData)
    
    model_mh.approxSimilarityJoin(featurizedData, featurizedData1, 0.7, "JaccardDistance")
      .select(
        col("datasetA.sentence").alias("idA"),
        col("datasetB.sentence").alias("idB"),
        col("JaccardDistance")).show(false)

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/winutils")
    process_streamingData()
  }

}

case class news(category: String, description: String)
case class newstime(time: String, category: String, description: String)