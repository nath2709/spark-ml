package ml.prediction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.math.{ sin, cos, toRadians, pow, sqrt, atan2 }
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression

object FarePrediction {

  //Haversine
  //formula:	a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
  //c = 2 ⋅ atan2( √a, √(1−a) )
  //d = R ⋅ c
  val distance = (pickup_longitude: Double,
    pickup_latitude: Double, dropoff_longitude: Double, dropoff_latitude: Double) => {
    val R = 6372.8 //radius in km
    val pick_lat_rad = toRadians(pickup_latitude)
    val pick_lon_rad = toRadians(pickup_longitude)
    val drop_lat_rad = toRadians(dropoff_latitude)
    val drop_long_rad = toRadians(dropoff_longitude)
    val delta_pick = toRadians(pick_lat_rad - pick_lon_rad)
    val delta_drop = toRadians(drop_lat_rad - drop_long_rad)

    val a = pow(sin(delta_pick / 2), 2) + cos(pick_lat_rad) * cos(pick_lon_rad) * pow(delta_drop / 2, 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    R * c

  }

  def processData(): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("fare-prediction-app").master("local[*]")
      .getOrCreate()

    val schema = Encoders.product[fare].schema
    import spark.implicits._

    val train_ds = spark.read.option("sep", ",").schema(schema).option("header", true).csv("D:/Kaggle/NewYorkCityTaxiFare/train_100k.csv").as[fare]

    //    train_ds.describe().show()

    val train_ds_fare = train_ds.filter($"label" > 0)
    val distance_udf = udf(distance)
    val train_ds_dist = train_ds_fare.withColumn("distance", distance_udf($"pickup_longitude", $"pickup_latitude", $"dropoff_longitude", $"dropoff_latitude"))
    //    println(train_ds_fare.count())

    val train_ds_dist_filtered = train_ds_dist.filter($"distance" > 0)
    val train_ds_dist_hour = train_ds_dist_filtered.withColumn("hour", hour(unix_timestamp($"pickup_datetime").cast("timestamp")))
    //    train_ds_dist_hour.describe().show()

    val discretizer = new QuantileDiscretizer().setInputCol("hour").setOutputCol("hourbin").setNumBuckets(4)
    val vector = new VectorAssembler().setInputCols(Array("hourbin", "distance")).setOutputCol("features")
    val regression = new LinearRegression().setMaxIter(3).setElasticNetParam(0.8).setRegParam(0.3)
    val pipeline = new Pipeline().setStages(Array(discretizer, vector, regression))

    val model = pipeline.fit(train_ds_dist_hour)

    val test_ds = spark.read.option("sep", ",").schema(schema).option("header", true).csv("D:/Kaggle/NewYorkCityTaxiFare/train_100k.csv").as[fare]

    //    train_ds.describe().show()

    val test_ds_fare = test_ds.filter($"label" > 0)
    val test_ds_dist = test_ds_fare.withColumn("distance", distance_udf($"pickup_longitude", $"pickup_latitude", $"dropoff_longitude", $"dropoff_latitude"))
    //    println(train_ds_fare.count())

    val test_ds_dist_filtered = test_ds_dist.filter($"distance" > 0)
    val test_ds_dist_hour = test_ds_dist_filtered.withColumn("hour", hour(unix_timestamp($"pickup_datetime").cast("timestamp")))
    
    val prediction = model.transform(test_ds_dist_hour).show()

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/winutils")
    processData()

    //    println("hello")
  }

  case class fare(key: String, label: Double, pickup_datetime: String, pickup_longitude: Double,
                  pickup_latitude: Double, dropoff_longitude: Double, dropoff_latitude: Double, passenger_count: Int)
}