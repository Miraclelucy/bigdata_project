package org.training.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.tools.cmd.Spec;

/**
 * Created by lushiqin on 5/21/19.
 * 统计HTTP日志返回代码:打印出总数，400的个数，200的个数
 * 累加器和JavaRDD.foreach()的用法
 */
public class StatisticsLog {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("StatisticsLog")
                .setMaster("local[1]");

        JavaSparkContext  sc=new JavaSparkContext(conf);

        String datapath="data/access.log";

        //1 create JavaRDD
        JavaRDD<String> input=sc.textFile(datapath);

        //2 create Accumulator
        Accumulator<Integer> total400=sc.accumulator(0);
        Accumulator<Integer> total200=sc.accumulator(0);
        //定义累加器的另一种方式
        //LongAccumulator accum = sc.sc().longAccumulator();


        //3
        input.map(x->x.split(","))
            .foreach(x->{
                if(Integer.parseInt(x[0])==400){
                    total400.add(1);
                }
                else if(Integer.parseInt(x[0])==200){ //must be Integer
                    total200.add(1);
                }
            });

        System.out.println("total400:"+total400.value());
        System.out.println("total200:"+total200.value());

        sc.stop();
    }
}
