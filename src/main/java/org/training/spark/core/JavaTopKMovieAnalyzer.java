package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by lushiqin on 5/15/19.
 * 得分最高的10部电影；看过电影最多的前10个人；女性看多最多的10部电影；男性看过最多的10部电影
 */
public class JavaTopKMovieAnalyzer
{
    public static void main(String[] args) {
        String masterurl="local[1]";
        String appname="JavaTopKMovieAnalyzer";
        String dataPath = "data/ml-1m";

        SparkConf sparkConf=new SparkConf();
        sparkConf.setMaster(masterurl).setAppName(appname);

        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        //1 create rdd
        JavaRDD<String>  ratingrdd=sc.textFile(dataPath+"/ratings.dat");
        JavaRDD<String>  usersrdd=sc.textFile(dataPath+"/users.dat");


        //2 top 10 movies that gets the highest score
        JavaRDD<Tuple3<String,String,String>> top10Movies=ratingrdd.map(x->x.split("::"))
                .map(x->new Tuple3<>(x[0],x[1],x[2])); //RDD(userid,movieid,Rating)

        top10Movies.mapToPair(x ->
                new Tuple2<String, Tuple2<Integer, Integer>>(x._1(),
                        new Tuple2<>(Integer.parseInt(x._3()), 1)))//RDD(userid,(rating,1))
                .reduceByKey((x,y)->new Tuple2<>(x._1()+y._1(),x._2()+y._2()))//RDD(userid,(sum(rating),sum(user)))
                .mapToPair(x->new Tuple2<>(x._2()._1()/x._2()._2()+0.0f,x._1()))//RDD(score,userid)
                .sortByKey(false) //sort by desc
                .take(10)
                .forEach(x -> System.out.println(x));

        //3 top 10 persons who watch movies most
        List<Tuple2<Integer,String>> top10WatchMost= ratingrdd
            .map(x->x.split("::"))
            .mapToPair(x->new Tuple2<>(x[0],1)) //RDD(userid,count)
            .reduceByKey((x,y)->x+y) //RDD(userid,counts)
            .mapToPair(x->new Tuple2<>(x._2(),x._1())) //RDD(counts,userid)
            .sortByKey(false)
            .take(10);

        top10WatchMost.stream()
                 .forEach(x->System.out.println(" top 10 persons who watch movies most :"+x));


         //4 top 10 movies that female watch most
        JavaPairRDD<String,String> usergenderrdd=usersrdd
            .map(x->x.split("::"))
            .mapToPair(x->new Tuple2<>(x[0],x[1]));//RDD(userid,gender)

        List<String> femalelist=usergenderrdd
            .filter(x->x._2().equals("F"))
                .map(x->x._1()).collect();//userid
        Set<String> femaleset=new HashSet<>();
        femaleset.addAll(femalelist);

        Broadcast<Set<String>> broadcastfemale=sc.broadcast(femaleset);

        List<Tuple2<Integer,String>> top10moviesfemale =ratingrdd.map(x->x.split("::"))
                .mapToPair(x->new Tuple2<>(x[0],x[1])) //RDD(userid,movieid)
                .filter(x->broadcastfemale.getValue().contains(x._1()))
                .mapToPair(x->new Tuple2<>(x._2(),1)) //RDD(movieid,count)
                .reduceByKey((x,y)->x+y) //RDD(movieid,counts)
                .mapToPair(x->new Tuple2<>(x._2(),x._1())) //RDD(count,movieid)
                .sortByKey(false)
                .take(10); //RDD(movieid,counts)

        top10moviesfemale.stream()
                .forEach(x->System.out.println("top 10 movies that female watch most:"+x));



    }
}
