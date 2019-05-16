package org.training.spark.core;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by lushiqin on 4/25/19.
 */
public class Java8WordCount {
    public static void main(String[] args)  throws IOException{
        String masterUrl="local[1]";
        String inputFile="data/textfile";
        String outputFile="output";

        if (args.length > 0) {
            masterUrl = args[0];
        } else if(args.length > 2) {
            inputFile = args[1];
            outputFile = args[2];
        }

        //1 create JavaSparkContext
        SparkConf sparkConf=new SparkConf().setAppName("Java8WordCount").setMaster(masterUrl);
        JavaSparkContext sc= new JavaSparkContext(sparkConf);

        //2 create JavaRDD
        JavaRDD<String> input=sc.textFile(inputFile);

        //3 transfrom JavaRDD
        JavaRDD<String>  wordrdd=input.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                                    .filter(s->s.length()>1);

        //4 create JavaPairRDD
        JavaPairRDD<String,Integer> countrdd=wordrdd.mapToPair(x-> new Tuple2<String,Integer>(x,1))
                                                .reduceByKey((x1,x2)->x1+x2);

        //5 save to text

        Path path=new Path(outputFile);
        FileSystem fs=path.getFileSystem(new HdfsConfiguration());
        if(fs.isDirectory(path)){
            fs.delete(path,true);
        }

        countrdd.saveAsTextFile(outputFile);

    }

}
