import org.apache.spark.streaming._

/**
  * Created by ganeshchand on 2/10/16.
  */

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

/**
  * Created by ganeshchand on 2/10/16.
  */
object FizzBuzzStreamStateful {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StatefulFizzBuzz").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 2222)
    ssc.checkpoint("/tmp/spark/checkpoint")

    val numbers = lines.flatMap(_.split(" "))

    def transform(n: Int): String = {
      if (n % 3 == 0 && n % 5 == 0) "Fizz Buzz"
      else if (n % 3 == 0) "Fizz"
      else if (n % 5 == 0) "Buzz"
      else n.toString
    }

    def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }

    val fizzBuzzKV = numbers.map(n => {
      (transform(n.toInt), 1)
    })

    val fizzBuzzCount = fizzBuzzKV.reduceByKey(_ + _)
    val cumulativefizzBuzzCount = fizzBuzzCount.updateStateByKey(updateFunction _)

    // print
     cumulativefizzBuzzCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

