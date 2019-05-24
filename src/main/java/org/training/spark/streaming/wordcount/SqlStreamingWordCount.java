package org.training.spark.streaming.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.training.spark.streaming.utils.Record;

import java.util.Arrays;

/**
 * Created by bigdata on 5/23/19.
 * 利用spark-sql处理流式数据
 */
public class SqlStreamingWordCount {

    public static void main(String[] args)  throws  InterruptedException{

        SparkConf conf= new SparkConf()
                .setAppName("SqlStreamingWordCount")
                .setMaster("local[2]");

        JavaStreamingContext jsc =new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> words=jsc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_2());

        JavaDStream<String> stream=words.flatMap(x-> Arrays.asList(x.split(" ")).iterator());

        //将流式数据利用spark-sql处理
        stream.foreachRDD((rdd,time)->{
            SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
            //1 将RDD转换成DataFrame
            JavaRDD<Record> records = rdd.map(w -> new Record(w));
            Dataset<Row> wordsDataFrame =
                    spark.createDataFrame(records, Record.class);
            //2 使用DataFrame创建临时表
            wordsDataFrame.createOrReplaceTempView("words");
            //3 利用spark-sql处理
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");

            System.out.printf("========= %d =========\n", time.milliseconds());
            wordCountsDataFrame.show();
        });

        //pairDStream.print();

        jsc.start();
        jsc.awaitTermination();
    }
}
