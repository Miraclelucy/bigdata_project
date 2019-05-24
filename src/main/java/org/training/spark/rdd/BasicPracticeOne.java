package org.training.spark.rdd;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lushiqin on 5/21/19.
 •  输出input中前5个数据  --take
 •	输出input中所有元素和  --reduce
 •	输出input中所有元素的平均值 --reduce --count
 •	统计input中偶数的个数，并打印前5个 --filter
 */
public class BasicPracticeOne {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setMaster("local[1]")
                .setAppName("BasicPracticeOne");

        JavaSparkContext sc= new JavaSparkContext(conf);

        List<Integer> list=new ArrayList<>();
        for(int i=100;i<=1000;i++){
            list.add(i);
        }
        JavaRDD<Integer> input =sc.parallelize(list,7);

        //1 输出input中前5个数据
        input.take(5).forEach(x->System.out.println("the front five number is "+x));

        //2 输出input中所有元素和
        int total = input.reduce((x,y)->x+y);
        System.out.println("the sum is "+total);

        //3 输出input中所有元素的平均值
        System.out.println("the avg is "+total/list.size());

        Accumulator<Integer> oddTotal=sc.accumulator(0);

        //4 统计input中偶数的个数，并打印前5个
        input.filter(x->{
            boolean flag=(x%2==0)?true:false;
             if(flag){
                 oddTotal.add(1);
             }
             return flag;
        }).take(5).stream().forEach(x->System.out.println("the front five even is "+x));

        System.out.println("the  even  nums is "+oddTotal.value());

    }
}
