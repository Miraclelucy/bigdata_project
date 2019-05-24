package org.training.spark.streaming.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by lushiqin on 5/23/19.
 * 从端口中接收流式数据，统计词频,统计过去5分钟内的单词计数,窗口大小是5分钟，滑动窗口是1分钟
 */
public class JavaStreamingWordCount2 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf= new SparkConf()
                .setAppName("JavaStreamingWordCount2")
                .setMaster("local[2]");

        JavaStreamingContext jsc =new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> words=jsc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_2());

        JavaDStream<String> stream=words.window(Durations.minutes(5), Durations.seconds(60)) //窗口大小是5分钟，滑动窗口是1分钟
                                        .flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream pairDStream=stream.mapToPair(x->new Tuple2<>(x,1))//DStream(word,1)
                                        .reduceByKey((x,y)->x+y); // DStream(word,count)

        pairDStream.print();

        jsc.start();
        jsc.awaitTermination();

    }

}
