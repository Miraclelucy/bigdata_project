package org.training.spark.streaming.wordcount;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.training.spark.streaming.utils.ConnectionPool;
import org.training.spark.streaming.utils.HBaseDao;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Created by bigdata on 5/23/19.
 * 将流式数据保存到Mysql中
 */
public class MysqlStreamingWordCount {
    public static void main(String[] args)  throws  InterruptedException{
        SparkConf conf= new SparkConf()
                .setAppName("MysqlStreamingWordCount")
                .setMaster("local[2]");

        JavaStreamingContext jsc =new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> words=jsc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_2());

        JavaDStream<String> stream=words.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairDStream=stream.mapToPair(x->new Tuple2<>(x,1))//DStream(word,1)
                .reduceByKey((x,y)->x+y); // DStream(word,count)

        //将流式数据保存到Mysql中
        pairDStream.foreachRDD((rdd,time)->{
            rdd.foreachPartition(record->{
                Connection conn = ConnectionPool.getConnection();
                record.forEachRemaining(new Consumer<Tuple2<String, Integer>>() {
                    @Override
                    public void accept(Tuple2<String, Integer> wc) {
                        try {
                            Statement st= conn.createStatement();
                            String sql="insert into streaming(word,count) values('" + wc._1 + "'," + wc._2 + ")";
                            st.execute(sql);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                });

                ConnectionPool.returnConnection(conn);

            });
        });

        //pairDStream.print();

        jsc.start();
        jsc.awaitTermination();

    }
}
