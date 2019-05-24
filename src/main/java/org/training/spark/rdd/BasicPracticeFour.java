package org.training.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Arrays;
import java.util.List;

/**
 * Created by lushiqin on 5/22/19.
 * 沃尔玛交易流水分析
 * 分店ID（id），交易量（x），交易额（y）和利润（z），类型均为整数
 *
 * 用RDD的transformation函数实现下列功能
 * SELECT id, SUM(x), MAX(y), MIN(z), AVERAGE(x) FROM T GROUP BY id
 *
 * mapValues()和两个对象之间reduceByKey()
 *
 * 难度指数：*****
 */
public class BasicPracticeFour {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setMaster("local[1]")
                .setAppName("BasicPracticeFour");

        JavaSparkContext sc= new JavaSparkContext(conf);

        String inputpath="data/input.txt";

        JavaRDD<String> inputrdd= sc.textFile(inputpath);

        JavaPairRDD<Long, SumMaxMin> resultrdd=inputrdd
                .mapToPair(s-> {
                    String[] row=s.split(",");
                    Long id = Long.parseLong(row[0]);
                    Long x = Long.parseLong(row[1]);
                    Long y = Long.parseLong(row[2]);
                    Long z = Long.parseLong(row[3]);
                    return  new Tuple2<>(id,new Tuple3<>(x,y,z));
                })
                .mapValues(v->new SumMaxMin(1,v._1(),v._2(),v._3()))
                .reduceByKey(SumMaxMin::add);


        resultrdd.foreach(x->{
                Long id=x._1();
                SumMaxMin sumMaxMin=x._2();
                System.out.println("id:"+id
                        +" sum:" +sumMaxMin.sum
                        +" max:"+sumMaxMin.max
                        +" min:"+sumMaxMin.min
                        +" avg:"+sumMaxMin.avrage());
        });
    }
}

class SumMaxMin implements Serializable {
    final int count;
    final long sum;
    final long max;
    final long min;

    public  SumMaxMin(int count,long sum,long max,long min){
        this.count=count;
        this.sum=sum;
        this.max=max;
        this.min=min;
    }

    public double avrage() {
        double result;
        if (count != 0) {
            result = sum*1.0 / count ;
        } else {
            result = 0.0;
        }
        return  result;
    }

    public SumMaxMin add(SumMaxMin other) {
        return new SumMaxMin(
                this.count + other.count,
                this.sum + other.sum,
                Math.max(this.max, other.max),
                Math.min(this.min, other.min)
                );
    }

}
