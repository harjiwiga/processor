package co.id.lsa.proc

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.{SparkConf, SparkContext}
import unicredit.spark.hbase.HBaseConfig
import unicredit.spark.hbase._


/**
  * Created by harji on 3/7/16.
  */
object MainHbaseProcess {


  private val sparkConf = new SparkConf().setAppName("HbaseProcessor").setMaster("local").set("spark.executor.memory", "1g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  private val jsc = new SparkContext(sparkConf)

  def main(args: Array[String]) {


    implicit val config = HBaseConfig()
    val hbasecon = config.get
    hbasecon.addResource("processor-conf.xml")
    val tablename2 = hbasecon.get("hbase.tablename")
    println("hbase.rootdir:" + hbasecon.get("hbase.rootdir"))
    val hbaseAdmin = new HBaseAdmin(hbasecon)

    if (hbaseAdmin.isTableAvailable(tablename2)) {

      val families = Set("f", "prod", "comm_t")
      val columns = Map(
        "prod" -> Set("desc", "sn")
      )
      val tableRdd2 = jsc.hbaseTS[String](tablename2, families)
//      val rddpart = tableRdd2.partitions


      val rddfiltered = tableRdd2.filter(x => x._2.get("prod").exists(_.get("sn").nonEmpty))

      val rddconverted = rddfiltered.map { eachVal => {
        val sn = eachVal._2.get("prod").get("sn")._1.filter(v => !v.equals(""))
        val cat = eachVal._2.get("prod").get("cat")._1
        val newVal = sn.replace("rb", "00").replace(",", "")
        val prot = eachVal._2.get("f").get("prot")._1
        val price = eachVal._2.get("prod").get("prc")._1

//          eachVal._2.get("prod").exists(_.exists(_._1=="prc"))
//          "" +
//          "//://._1.replace("Rp","").replaceAll(".","")
        (eachVal._1,cat,newVal,prot,price)
      }
      }

      rddfiltered.foreach(i => println("filtered_data:" + (i._2.get("prod").get("sn")._1)))

      rddconverted.foreach(i => println("convertedData: " + i._5))
      println("valuess:" + rddfiltered.count())
      println("class rdd: " + tableRdd2.getClass)
    }


    hbaseAdmin.close();
    jsc.stop();
    System.exit(0);
  }

  def convert(sn: String): String = {
    sn.replace(",", "").replace("rb", "00")
  }
}
