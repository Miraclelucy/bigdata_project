package org.training.spark.streaming.wordcount;

import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by lushiqin on 5/11/19.
 * 从端口中接收流式数据，统计词频,统计过去1分钟内的单词计数,
 *
 */
public class JavaStreamingWordCount {

    public static void main(String[] args) throws InterruptedException {

        if(args.length<2){
            System.err.println("args nums error");
            System.exit(1);
        }

        //1 create JavaStreamingContext whit 1 second batch size
        SparkConf sparkconf=new SparkConf().setAppName("JavaStreamingWordCount");
        sparkconf.setMaster("local[2]");
        //JavaStreamingContext ssc=new JavaStreamingContext(sparkconf, Durations.seconds(10));
        JavaStreamingContext ssc=new JavaStreamingContext(sparkconf, Durations.minutes(1));
        ssc.sparkContext().setLogLevel("WARN");

        //2 create JavaReceiverInputDStream  receive data from socket
        JavaReceiverInputDStream<String>  lines=ssc.socketTextStream(args[0],Integer.parseInt(args[1]), StorageLevel.MEMORY_AND_DISK_SER());

        //3 create JavaDStream process data to arrary
        JavaDStream<String> words=lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator());

        //4 create JavaPairDStream process data for wordcount
        JavaPairDStream<String,Integer> wordCount=words.mapToPair(x->new Tuple2<>(x,1)).reduceByKey((x1,x2)->x1+x2);


        wordCount.print();
        //wordCount.saveAsHadoopFiles();
        ssc.start();
        ssc.awaitTermination();



    }



}
