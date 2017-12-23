package sparkEx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner

object MovingAvg extends App {

  val conf = new SparkConf().setAppName("").setMaster("local")
  val sc = new SparkContext(conf)

  val templines = List("abc,2017-10-04,15.2",
    "abc,2017-10-03,19.67",
    "abc,2017-10-02,19.8",
    "xyz,2017-10-09,46.9",
    "xyz,2017-10-08,48.4",
    "xyz,2017-10-07,87.5",
    "xyz,2017-10-04,83.03",
    "xyz,2017-10-03,83.41",
    "pqr,2017-09-30,18.18",
    "pqr,2017-09-27,18.2",
    "pqr,2017-09-26,19.2",
    "pqr,2017-09-25,19.47",
    "abc,2017-07-19,96.60",
    "abc,2017-07-18,91.68",
    "abc,2017-07-17,91.55")
  val rdd = sc.parallelize(templines)
  val rows = rdd.map(line => {
    val row = line.split(",")
    ((row(0), row(1)), row(2))
  })

  val op = rows.repartitionAndSortWithinPartitions(new CustomPartitioner(4))
  val temp = op.map(f => (f._1._1, (f._1._2, f._2)))
    temp.foreach(println)

  val mergeCombiners = (t1: (String, List[String]), t2: (String, List[String])) => (t1._1 + t2._1, t1._2.++(t2._2))
  val mergeValue = (x: (String, List[String]), y: (String, String)) => {
    val a = x._2.+:(y._2)
    (x._1, a)
  }
  //    val mergeCombiners = (x:List[String],y:List[String]) => x.++(y)
  //      createCombiner:(String,String) => C,
  //       mergeValue: (C, (String,String)) => C,
  //      mergeCombiners: (C, C) => C

//  val x = temp.combineByKey(
//    //        createCombiner,mergeValue,mergeCombiners
//    (t1: String, t2: String) => (t1, List(t2)),
//    mergeValue,
//    mergeCombiners)

  //  val rdd = sc.parallelize(List(
  //    ("A", 3), ("A", 9), ("A", 12), ("A", 0), ("A", 5), ("B", 4),
  //    ("B", 10), ("B", 11), ("B", 20), ("B", 25), ("C", 32), ("C", 91),
  //    ("C", 122), ("C", 3), ("C", 55)), 2)
  //
  // val x = rdd.combineByKey(
  //    (x: Int) => (x, 1),
  //    (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
  //    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

}
