package sparkEx

object fp {

  def fibw(n: Int): Int = {

    def loop(n: Int): Int = {
      if (n <=1) n
       
      else {
//        println(n)
        loop(n - 1) + loop(n - 2)
      }
    }
    loop(n)  
  }

  def fib(n: Int): Int = {
    @annotation.tailrec
    def loop(n: Int, prev: Int, cur: Int): Int =
      if (n <= 0) prev

      else loop(n - 1, cur, prev + cur)

    loop(n, 0, 1)
  }

  def main(args: Array[String]): Unit = {
    println(fibw(5))
  }
}