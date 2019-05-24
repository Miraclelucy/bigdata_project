package org.training.spark.streaming.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Option;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by lushiqin on 5/23/19.
 * 统计历史上所有单词的总数
 * Checkpoint检查点，如何做Checkpoint，mapWithState的用法
 *
 * mapWithState是spark2.0中的，(效率更高，生产中建议使用)
 * mapWithState：也是用于全局统计key的状态，但是它如果没有数据输入，便不会返回之前的key的状态，有一点增量的感觉。
 * 这样做的好处是，我们可以只是关心那些已经发生的变化的key，对于没有数据输入，则不会返回那些没有变化的key的数据。
 * 这样的话，即使数据量很大，checkpoint也不会像updateStateByKey那样，占用太多的存储。
 *
 * 难度指数：*****
 */
public class StatefulStreamingWordCount {
    public static void main(String[] args) throws InterruptedException {
        String checkpointpath="checkpoint";
        SparkConf conf= new SparkConf()
                .setAppName("StatefulStreamingWordCount")
                .setMaster("local[2]");

        JavaStreamingContext jsc =new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.sparkContext().setLogLevel("WARN");
        jsc.checkpoint(checkpointpath);

        JavaReceiverInputDStream<String> words=jsc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_2());

        JavaPairDStream<String,Integer> stream=words.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x->new Tuple2<>(x,1));//DStream(word,1)

        //1 方法一 mapWithState的用法
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateWordCounts =
                stream.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) throws Exception {
                        Option<Integer> stateCount = state.getOption();
                        Integer sum = one.orElse(0);
                        if (stateCount.isDefined()) {
                            sum += stateCount.get();
                        }
                        state.update(sum);
                        return new Tuple2<String, Integer>(word, sum);
                    }
                }));




        stateWordCounts.print();

        jsc.start();
        jsc.awaitTermination();

    }
}
