package org.training.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by lushiqin on 5/22/19.
 * 计算相同Key对应的的所有value的平均值，并输出到目录/tmp/output下
 * SELECT id,  AVERAGE(x) FROM T GROUP BY id
 *
 *  mapValues()和两个对象之间reduceByKey()
 *
 * 难度指数：****
 */
public class BasicPracticeThree {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setMaster("local[1]")
                .setAppName("basicPracticeThree");

        JavaSparkContext sc= new JavaSparkContext(conf);

        sc.setLogLevel("error");
        List<Tuple2<String, Integer>> data = Arrays.asList(
                new Tuple2("coffee", 1),
                new Tuple2("coffee", 3),
                new Tuple2("panda", 4),
                new Tuple2("coffee", 5),
                new Tuple2("street", 2),
                new Tuple2("panda", 5)
        );

        JavaPairRDD<String,Integer> inputrdd=sc.parallelizePairs(data);

        //1
        JavaPairRDD<String,Double> resultrdd= inputrdd.mapValues(x->new SumAndCount(x,1))
                .reduceByKey((x,y)->new SumAndCount(x.sum+y.sum,x.count+y.count))
                .mapValues(x->x.avrage(x.sum,x.count));

        resultrdd.foreach(x->System.out.println(x));

        //2
        JavaPairRDD<String,SumAndCount> result2rdd= inputrdd.mapValues(x->new SumAndCount(x,1))
                .reduceByKey(SumAndCount::add);

        sc.stop();
    }
}


class SumAndCount implements Serializable{
    final int sum;
    final int count;

    public  SumAndCount(int sum,int count){
        this.sum=sum;
        this.count=count;
    }

    public double avrage(int sum,int count) {
        double result;
        if (count != 0) {
            result = sum*1.0 / count ;
        } else {
            result = 0.0;
        }
        return  result;
    }

    public SumAndCount add(SumAndCount other) {
        return new SumAndCount(
                this.count + other.count,
                this.sum + other.sum
        );
    }

}