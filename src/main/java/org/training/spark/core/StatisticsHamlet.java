package org.training.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lushiqin on 5/21/19.
 * 哈姆雷特词频分析
 * 累加器和广播变量的应用,JavaRDD.filter()的用法
 */
public class StatisticsHamlet {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf()
                .setAppName("StatisticsHamlet")
                .setMaster("local[1]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);

        String hamletpath="data/textfile/Hamlet.txt";
        String stopwordpath="data/textfile/stopword.txt";

        JavaRDD<String> inputhamlet =sc.textFile(hamletpath);
        JavaRDD<String> inputstopword =sc.textFile(stopwordpath);

        Accumulator<Integer> countTotal = sc.accumulator(0);
        Accumulator<Integer> stopTotal = sc.accumulator(0);
        //定义累加器的另一种方式
        //LongAccumulator accum = sc.sc().longAccumulator();


        //create broadcast variable
        List<String> stoplist=inputstopword.collect();
        Broadcast<List<String>> stopwordbroadcast=sc.broadcast(stoplist);

        JavaRDD<String> hamletrdd =inputhamlet
                .flatMap(x-> splitWords(x).iterator());  //

        hamletrdd
                .filter(x->{
                    countTotal.add(1);
                    boolean flag=stopwordbroadcast.getValue().contains(x);
                    if(flag){
                        stopTotal.add(1);
                    }
                    return !flag;
                }) // filter  the stop word
                .mapToPair(x->new Tuple2<>(x,1))
                .reduceByKey((x, y)->x+y) //RDD(word,count)
                .mapToPair(x->new Tuple2<>(x._2(),x._1()))//RDD(count,word)
                .sortByKey(false) // sort by desc
                .take(10) // take top 10
                .stream()
                .forEach(x->System.out.println(x));
        System.out.println("countTotal:"+countTotal.value());
        System.out.println("stopTotal:"+stopTotal.value());
    }

    /***
     * process one line data
     * @param line
     * @return
     */
    public static List<String> splitWords(String line) {
        List<String> result = new ArrayList<String>();
        String[] words = line.replaceAll("['.,:?!-]", "").split("\\s");
        for (String w: words) {
            if (!w.trim().isEmpty()) {
                result.add(w.trim());
            }
        }
        return result;
    }

}
