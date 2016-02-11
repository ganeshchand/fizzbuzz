

/**
  * Created by ganeshchand on 2/10/16.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 2222`
  * and then run the example
  *    `$ bin/spark-submit --class FizzBuzzStream /path-to/fizzbuzz-1.0-SNAPSHOT.jar`
  *
  * For input, in the terminal where netcat server is running, enter numbers separated by space ending with \n
  *    e.g
  *    1 1 1 1 3 3 3 10 15
  *
  *
  * The program terminates after 5 sec.
  *
  *
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

