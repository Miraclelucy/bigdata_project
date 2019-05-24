package org.training.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lushiqin on 5/21/19.
 •	使用union连接rdd1和rdd2，生成rdd4  --union
 •	使用glom打印rdd4的各个partition  --glom  合并RDD的每个partition里的元素为一个list
 •	使用coalesce将rdd4的分区数改为3，并生成rdd5  --coalesce  缩减分区个数
 •	使用repartition将rdd4的分区数改为10，并生成rdd6  --repartition
 •	使用glom分别打印rdd5和rdd6中的partition元素均匀性 --glom
 */
public class BasicPracticeTwo {
    public static void main(String[] args) {

        SparkConf conf=new SparkConf()
                .setMaster("local[1]")
                .setAppName("BasicPracticeTwo");

        JavaSparkContext sc= new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        List<Integer> data = Arrays.asList(1,2,3,4,5, 6);
        JavaRDD<Integer> rdd1 = sc.parallelize(data, 3);
        List<Integer> data2 =Arrays.asList(7,8,9,10,11);
        JavaRDD<Integer> rdd2 = sc.parallelize(data, 2);
        List<Integer> data3=Arrays.asList(12,13,14,15,16, 17, 18, 19, 20, 21);
        JavaRDD<Integer> rdd3 = sc.parallelize(data3, 3);

        //1 使用union连接rdd1和rdd2，生成rdd4
        JavaRDD<Integer> rdd4=rdd1.union(rdd2);
        rdd4.foreach(x->System.out.println("rdd4 is "+x));

        //2 使用glom打印rdd4的各个partition
        rdd4.glom().foreach(x->System.out.println("the partition of rdd4 is "+x));

        //3 使用coalesce将rdd4的分区数改为3，并生成rdd5
        JavaRDD<Integer> rdd5=rdd4.coalesce(3);

        //4 使用repartition将rdd4的分区数改为10，并生成rdd6
        JavaRDD<Integer> rdd6=rdd4.repartition(10);

        //5 使用glom分别打印rdd5和rdd6中的partition元素均匀性
        rdd5.glom().foreach(x->System.out.println("the partition of rdd5 is "+x));
        rdd6.glom().foreach(x->System.out.println("the partition of rdd6 is "+x));

        sc.stop();
    }

}
