package sparkEx

import scala.collection.immutable.Queue

object SimpleMovingAvg {
  
  var sum = 0.0
  val period = 3
  val window : Queue[Double] = Queue()
  
  def addNewnumber(number:Double){
    
    sum+=number
    window.enqueue(number)
    if(window.size > period){
      window.dequeue
      
    }
  }
}