package com.lsa.id.spark.main

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

/**
  * Created by harji on 3/7/16.
  */
class HbaseProccessor {

  private[main] var sparkConf: SparkConf = new SparkConf().setAppName("HbaseProcessor").setMaster("local");
  private[main] var jsc: JavaSparkContext = new JavaSparkContext(sparkConf);

  def main(args: Array[String]) {


    val conf = HBaseConfiguration.create();

    conf.addResource("hbase-site.xml");
    val tableName = conf.get("hbase.tablename");

    System.out.println("tableName: "+tableName);

    System.exit(0);
  }


}
