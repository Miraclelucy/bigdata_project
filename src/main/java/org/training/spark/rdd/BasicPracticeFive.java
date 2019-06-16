package org.training.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by lushiqin on 2019-05-30.
 * spark解决分组排序的问题
 */
public class BasicPracticeFive {
    public static void main(String[] args) {
        String masterUrl="local[1]";
        String inputFile="data/unsort.txt";
        String outputFile="output";

        if (args.length > 0) {
            masterUrl = args[0];
        } else if(args.length > 2) {
            inputFile = args[1];
            outputFile = args[2];
        }

        //1 create JavaSparkContext
        SparkConf sparkConf=new SparkConf().setAppName("BasicPracticeFive").setMaster(masterUrl);
        JavaSparkContext sc= new JavaSparkContext(sparkConf);

        //2 create JavaRDD
        JavaRDD<String> input=sc.textFile(inputFile);

        input.map(x->x.split(" "))
                .mapToPair(x->new Tuple2<String,String>(x[0],x[1]))
                .groupByKey()
                .map(x->{
                    Iterator<String> iterator = x._2().iterator();
                    List<String> list = new ArrayList<String>();
                    while (iterator.hasNext()) {
                        list.add(iterator.next());
                    }
                    Collections.sort(list);
                    return new Tuple2<String,Iterator<String>>(x._1(),list.iterator());
                }).foreach(x->{
                    String m=x._1();
                    x._2().forEachRemaining(n-> System.out.println(m+" "+n));
                });


    }
}
