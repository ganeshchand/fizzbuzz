

/**
  * Created by ganeshchand on 2/10/16.
  */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, _}

/**
  * Created by ganeshchand on 2/10/16.
  */
object FizzBuzzStream {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StatefulFizzBuzz").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 2222) // InputDStream

    val numbers = lines.flatMap(_.split(" "))

    def transform(n: Int): String = {
      if (n % 3 == 0 && n % 5 == 0) "Fizz Buzz"
      else if (n % 3 == 0) "Fizz"
      else if (n % 5 == 0) "Buzz"
      else n.toString
    }

    val fizzBuzzKV = numbers.map(n => {
      (transform(n.toInt), 1)
    })

    val fizzBuzzCount = fizzBuzzKV.reduceByKey(_ + _).transform(rdd => rdd.sortByKey(numPartitions = 1))

    // print
    fizzBuzzCount.foreachRDD(rdd => rdd.foreach(t => println(s"${t._1} - ${t._2}")))

//    print(fizzBuzzCount.transform(rdd=>rdd.sortByKey(numPartitions = 1)).formatted(" - "))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5000) // timeout in 5 sec
  }

}

