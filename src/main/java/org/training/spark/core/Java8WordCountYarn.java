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
import java.util.Arrays;

/**
 * Created by lushiqin on 5/14/19.

 1 uopload dataset
 hdfs dfs -put /app/bigdata-project/data/textfile/  /tmp/input

 2 submit jar
 ./spark-submit \
 --master yarn \
 --deploy-mode client \
 --class org.training.spark.core.Java8WordCountYarn \
 --name wordcount \
 /app/bigdata-project/target/bigdata-project-1.0-SNAPSHOT.jar  \

 */
public class Java8WordCountYarn {

    public static void main(String[] args) throws IOException {

        String inputFile="hdfs:///tmp/input";
        String outputFile="hdfs:///tmp/output";


       if(args.length == 2) {
            inputFile = args[0];
            outputFile = args[1];
        }

        //1 create JavaSparkContext
        SparkConf sparkConf=new SparkConf().setAppName("Java8WordCountYarn");
        JavaSparkContext sc= new JavaSparkContext(sparkConf);

        //2 create JavaRDD
        JavaRDD<String> input=sc.textFile(inputFile);

        //3 transfrom JavaRDD
        JavaRDD<String>  wordrdd=input.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                .filter(s->s.length()>1);

        //4 create JavaPairRDD
        JavaPairRDD<String,Integer> countrdd=wordrdd.mapToPair(x-> new Tuple2<String,Integer>(x,1))
                .reduceByKey((x1,x2)->x1+x2);

        countrdd.cache();
        //5 save to text

        Path path=new Path(outputFile);
        FileSystem fs=path.getFileSystem(new HdfsConfiguration());
        if(fs.isDirectory(path)){
            fs.delete(path,true);
        }

        countrdd.saveAsTextFile(outputFile);

    }
}
