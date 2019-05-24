package org.training.spark.streaming.wordcount;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.training.spark.streaming.utils.HBaseDao;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Created by lushiqin on 5/23/19.
 * 将流式数据保存到HBase中
 */
public class HBaseStreamingWordCount {
    public static void main(String[] args) throws InterruptedException  {
        SparkConf conf= new SparkConf()
                .setAppName("HBaseStreamingWordCount")
                .setMaster("local[2]");

        JavaStreamingContext jsc =new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> words=jsc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_2());

        JavaDStream<String> stream=words.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairDStream=stream.mapToPair(x->new Tuple2<>(x,1))//DStream(word,1)
                                                        .reduceByKey((x,y)->x+y); // DStream(word,count)

        //将流式数据保存到HBase中
        pairDStream.foreachRDD((rdd,time)->{
            rdd.foreachPartition(record->{
                HTable t1 = HBaseDao.getHTable("streaming_word_count");//
               // HTable t2 = HBaseDao.getHTable("streaming_word_count_incr");
                  record.forEachRemaining(new Consumer<Tuple2<String, Integer>>() {
                      @Override
                      public void accept(Tuple2<String, Integer> wc) {
                          try {
                            HBaseDao.save(t1, wc._1, wc._2, time.milliseconds());
                             // HBaseDao.incr(t2, wc._1, wc._2);
                          } catch (IOException e) {
                              e.printStackTrace();
                          }
                      }
                  });

                t1.close();
            });
        });

        //pairDStream.print();

        jsc.start();
        jsc.awaitTermination();

    }
}
