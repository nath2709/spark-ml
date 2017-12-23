package sparkEx

import org.apache.spark.Partitioner

class CustomPartitioner(numParts:Int) extends Partitioner {

  def numPartitions: Int = {
    numParts
  }
  def getPartition(key: Any): Int = {
   
    key.asInstanceOf[Tuple2[String,String]]._1.hashCode()%numParts
    
  }
}