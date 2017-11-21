package com.poc.clouderahbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import java.util.*;

/**
 * Created by hduser on 18/11/17.
 */
public class JavaHBase {

    public void readData(String tableName) {

        /*SparkSession session = SparkSession.builder()
                .master("local[4]")
                .appName("POC")
                .getOrCreate();

        SQLContext sqlContext = session.sqlContext();

        //String catalog1 = "{\"table\":{\"namespace\":\"default\",\"name\":\"test\"},\"rowkey\":\"key\",\"columns\":{\"col0\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"col1\":{\"cf\":\"cf\",\"col\":\"a\",\"type\":\"string\"},\"col2\":{\"cf\":\"cf\",\"col\":\"b\",\"type\":\"string\"},\"col3\":{\"cf\":\"cf\",\"col\":\"c\",\"type\":\"string\"}}}";

        Map<String, String> map = new HashMap<>();
        map.put("hbase.columns.mapping", "KEY_FIELD STRING :key, A_FIELD STRING c:a, B_FIELD STRING c:b");
        map.put("hbase.table", tableName);
        //map.put(HBaseTableCatalog.tableCatalog(), catalog1);

        *//*Dataset<Row> ds = sqlContext.load("org.apache.hadoop.hbase.spark", map);
        ds.registerTempTable("hbaseTmp");

        sqlContext.sql("SELECT KEY_FIELD FROM hbaseTmp " +
                "WHERE " +
                "(KEY_FIELD = 'get1' and B_FIELD < '3') or " +
                "(KEY_FIELD <= 'get3' and B_FIELD = '8')").collectAsList();*//*

        List<Row> rows = sqlContext.read().options(map).format("org.apache.hadoop.hbase.spark").load().collectAsList();
        System.out.println(rows.toString());*/

        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaHBaseBulkGetExample " + tableName)
                .setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        try {
            /*List<byte[]> list = new ArrayList<>(5);
            list.add(Bytes.toBytes("1"));
            list.add(Bytes.toBytes("2"));
            list.add(Bytes.toBytes("3"));
            list.add(Bytes.toBytes("4"));
            list.add(Bytes.toBytes("5"));

            JavaRDD<byte[]> rdd = jsc.parallelize(list);*/

            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.master", "localhost:60000");
            conf.set("hbase.zookeeper.quorum", "localhost");

            JavaHBaseContext hBaseContext = new JavaHBaseContext(jsc, conf);

            //hBaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetFunction(), new ResultFunction());

            Scan scan = new Scan();
            scan.setCaching(100);

            JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRdd = hBaseContext.hbaseRDD(TableName.valueOf(tableName), scan);


            List<String> results = javaRdd.map(new ScanConvertFunction()).collect();

            System.out.println("Result Size: " + results.size());



        } finally {
            jsc.stop();
        }
    }

    private static class ScanConvertFunction implements
            Function<Tuple2<ImmutableBytesWritable, Result>, String> {
        @Override
        public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
            return Bytes.toString(v1._1().copyBytes());
        }
    }

    public static class GetFunction implements Function<byte[], Get> {

        private static final long serialVersionUID = 1L;

        public Get call(byte[] v) throws Exception {
            return new Get(v);
        }
    }

    public static class ResultFunction implements Function<Result, String> {

        private static final long serialVersionUID = 1L;

        public String call(Result result) throws Exception {
            Iterator<Cell> it = result.listCells().iterator();
            StringBuilder b = new StringBuilder();

            b.append(Bytes.toString(result.getRow())).append(":");

            while (it.hasNext()) {
                Cell cell = it.next();
                String q = Bytes.toString(cell.getQualifierArray());
                if (q.equals("counter")) {
                    b.append("(")
                            .append(Bytes.toString(cell.getQualifierArray()))
                            .append(",")
                            .append(Bytes.toLong(cell.getValueArray()))
                            .append(")");
                } else {
                    b.append("(")
                            .append(Bytes.toString(cell.getQualifierArray()))
                            .append(",")
                            .append(Bytes.toString(cell.getValueArray()))
                            .append(")");
                }
            }
            return b.toString();
        }
    }
}
