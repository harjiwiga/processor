package com.lsa.id.spark.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

//import org.apache.hadoop.hbase.

/**
 * Created by harji on 3/4/16.
 */
public class HbaseProccess {

    static SparkConf sparkConf = new SparkConf().setAppName("HbaseProccss").setMaster("local");
    static JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    public static void main(String[] args) throws IOException {


        Configuration conf = HBaseConfiguration.create();
        conf.addResource("processor-conf.xml");

        System.out.println("tablename: "+conf.get("hbase.tablename"));

        String tableName = conf.get("hbase.tablename");
        conf.addResource("hbase-site.xml");
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        HTable table = new HTable(conf, tableName);

        HBaseAdmin admin = new HBaseAdmin(conf);
        System.out.println("table avaliable? :" + admin.isTableAvailable(tableName));

        if (admin.isTableAvailable(tableName)) {
            JavaPairRDD<ImmutableBytesWritable, Result> tableRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            System.out.println("count result: " + tableRdd.count());




//            tableRdd.
        }

        jsc.stop();
        System.exit(0);

    }


}
