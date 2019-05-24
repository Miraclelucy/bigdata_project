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
 * 看过“Lord of the Rings, The (1978)”用户和年龄性别分布
 */
public class JavaMovieUserAnalyzer {

    public static void main(String[] args)  throws IOException {
        String masterUrl="local[1]";
        String inputFile="data/ml-1m";
        String outputFile="output-movie";

        if (args.length > 0) {
            masterUrl = args[0];
        } else if(args.length > 2) {
            inputFile = args[1];
            outputFile = args[2];
        }

        //1 create JavaSparkContext
        SparkConf sparkConf=new SparkConf().setAppName("JavaMovieUserAnalyzer").setMaster(masterUrl);
        JavaSparkContext sc= new JavaSparkContext(sparkConf);

        //2 create JavaRDD
        String MOVIE_TITLE = "Lord of the Rings, The (1978)";
        final String MOVIE_ID = "2116";

        String userpath=inputFile+"/users.dat";
        String ratingspath=inputFile+"/ratings.dat";


        JavaRDD<String> userrdd=sc.textFile(userpath);
        JavaRDD<String> ratingrdd=sc.textFile(ratingspath);

        //3 Extract columns from RDDs
        //users: RDD[(userID, gender:age)]
        JavaPairRDD<String,String>  userpairrdd=userrdd.mapToPair(x->{
            String[] arraystr=x.split("::");
            return new Tuple2<>(arraystr[0],arraystr[1]+":"+arraystr[2]);
        });

        //rating: RDD[(userID,movieID)]
        JavaPairRDD<String,String>  ratingpairrdd=ratingrdd.mapToPair(x->{
            String[] arraystr=x.split("::");
            return new Tuple2<>(arraystr[0],arraystr[1]);
        }).filter(s->s._2().equals(MOVIE_ID));

        //4 join
        //useRating: RDD[(userID, (movieID, gender:age)]
        JavaPairRDD<String,Tuple2<String,String>> useRatingrdd= ratingpairrdd.join(userpairrdd);

        //userDistribution: RDD[(gender:age, count)]
        JavaPairRDD<String,Integer> resultrdd=useRatingrdd.mapToPair(x->{
            return  new Tuple2<>(x._2()._2(),1);
        }).reduceByKey((x,y)->x+y);


        //5 save as files
        Path outputPath = new Path(outputFile);
        FileSystem fs = outputPath.getFileSystem(new HdfsConfiguration());
        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        resultrdd.saveAsTextFile(outputFile);


        resultrdd.collect().stream().forEach(t ->
                System.out.println("gender & age:" + t._1() + ", count:" + t._2()));

    }

}
