package co.id.lsa.proc

import co.id.lsa.mapper.ProductWeb
import org.apache.spark.{SparkContext, SparkConf}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.Breaks

/**
  * Created by harji on 3/8/16.
  */
object MainTestScala {

   //  private val sparkConf = new SparkConf().setAppName("HbaseProcessor").setMaster("local").set("spark.executor.memory", "1g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   //  private val jsc = new SparkContext(sparkConf)


   def main(args: Array[String]) {
      val numbers = List(1, 2, 3, 4, 5, 6, 6)

      numbers.reverse.foreach {
         n: Int => println("Number: " + n)
      }
      //    numbers.foreach(n -> { System.out.println("Numbers "+n);});
      val fewNum = numbers.drop(2)
      val fewNum2 = numbers.take(3)

      print("drop num: " + fewNum)
      print("\ntake num:" + fewNum2.map(x => x * 4).sum)

      //    val words = Array("one", "two", "two", "three", "three", "three")
      //    val wordPairsRDD = jsc.parallelize(words).map(word => (word, 1))
      //
      //    val wordCountsWithReduce = wordPairsRDD
      //      .reduceByKey(_ + _)
      //      .collect()
      //
      //    val wordCountsWithGroup = wordPairsRDD
      //      .groupByKey()
      //      .map(t => (t._1, t._2.sum))
      //      .collect()
      //
      //    val prw = new ProductWeb()

      var arr = Array(2, 3, 1, 0, -1)
      arr = sort(arr)

      var a = sumInts(1, 5)
      println("\na:" + a)

      var b = sum(x => x)
      println("\nb:" + b)
      arr.foreach(println)
      //    println("\nid cust: "+customer.myFirstMethod())
      var exMap = mutable.HashMap("ab" -> "1", "abc" -> "2", "abcd" -> "3", "klm" -> "4","xyz" -> "4")
      //        exMap.isInstanceOf[Map]
      //      exMap.retain((k, v) => k.equals(e))
      //      exMap.foreach(x=>x._1)
      var keyset = exMap.keySet



//      println("max: "+keyset.groupBy())


      val loop = new Breaks;
      loop.breakable {
         for ((k, v) <- exMap) {
            for (key <- keyset) {
               if ((k.contains(key) && (k.length == key.length))) {
                  println("key: "+key)
                  println("m: " + (exMap - (key)))
                  loop.break;
               }
            }
         }
      }
//
      var newMap = mutable.HashMap
      for((k,v) <- exMap){
         var selKey =k;
         var lkey = k.length

      }


      var it = keyset.iterator
//      println("keyset3: "+keyset.)
      var arrset =keyset.toArray
      var i =0;



      exMap.foreach(println)


      var mutblSet = mutable.Set("ab","abc","abcd","klm")

       filter(mutblSet,mutblSet.min).foreach(println)

//      println("newset: "+newSet)

      System.exit(0)
   }

   def sort(xs: Array[Int]): Array[Int] = {
      if (xs.length <= 1) xs
      else {
         val pivot = xs(xs.length / 2)
         Array.concat(sort(xs.filter(pivot >)), xs.filter(pivot ==), sort(xs.filter(pivot <)))
      }
   }

   def sumInts(a: Int, b: Int): Int = if (a > b) 0 else a + sumInts(a + 1, b)

   def sum(f: Int => Int): (Int, Int) => Int = {
      def sumF(a: Int, b: Int): Int =
         if (a > b) 0 else f(a) + sumF(a + 1, b)
      sumF
   }

   def filter(a: mutable.Set[String], key:String): mutable.Set[String] = {
      var smin = a.min
     a
   }

   // 1 - using `match`
   def max(ints: List[Int]): Int = {
      @tailrec
      def maxAccum(ints: List[Int], theMax: Int): Int = {
         ints match {
            case Nil => theMax
            case x :: tail =>
               val newMax = if (x > theMax) x else theMax
               maxAccum(tail, newMax)
         }
      }
      maxAccum(ints, 0)
   }

   // 2 - using if/else
   def max2(ints: List[Int]): Int = {
      @tailrec
      def maxAccum2(ints: List[Int], theMax: Int): Int = {
         if (ints.isEmpty) {
            return theMax
         } else {
            val newMax = if (ints.head > theMax) ints.head else theMax
            maxAccum2(ints.tail, newMax)
         }
      }
      maxAccum2(ints, 0)
   }




}
