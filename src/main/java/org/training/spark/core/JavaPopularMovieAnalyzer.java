package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 * Created by lushiqin on 5/15/19.
 * 年龄段在“18-24”的男性年轻人，最喜欢看哪10部电影
 */
public class JavaPopularMovieAnalyzer
{

    public static void main(String[] args) {

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
        String gender="M";
        int minage = 18;
        int maxage = 24;

        String userpath=inputFile+"/users.dat";
        String ratingspath=inputFile+"/ratings.dat";
        String moviespath=inputFile+"/movies.dat";

        JavaRDD<String> userrdd=sc.textFile(userpath);
        JavaRDD<String> ratingrdd=sc.textFile(ratingspath);
        JavaRDD<String> movierdd=sc.textFile(moviespath);

        //3 Extract columns from RDDs
        //users:  RDD[(userID, gender, age)]
        JavaRDD<Tuple3<String,String,String>> userpairrdd=userrdd
                .map(x->{
                    String[] arraystr=x.split("::");
                    return new Tuple3<>(arraystr[0],arraystr[1],arraystr[2]);
                })
                .filter(x->x._2().equals(gender))
                .filter(x->{
                    int a = Integer.parseInt(x._3());
                    return  a >= minage && a <=maxage;
                });

        //userlist
        List<String> userlist=userpairrdd.map(x->x._1()).collect();

        //broadcast  userset
        Set<String> userset=new HashSet<>();
        userset.addAll(userlist);
        Broadcast<Set<String>> broadcastUserset=sc.broadcast(userset);

        //topMovies: RDD[(count,movieID)]->List<Tuple2<String,String>>
        List<Tuple2<Integer,String>>  topMovies=ratingrdd
                .map(x->x.split("::"))
                .mapToPair(x->new Tuple2<String,String>(x[0],x[1])) //(userID,movieID)
                .filter(x->broadcastUserset.getValue().contains(x._1()))
                .mapToPair(x->new Tuple2<String,Integer>(x._2(),1)) //(movieID,1)
                .reduceByKey((x,y)->x+y) //(movieID,count)
                .mapToPair(x->new Tuple2<Integer,String>(x._2(),x._1()))//(count,movieID)
                .sortByKey(false) // sort by key desc
                .take(10); // take top 10 for list

        //movielist
        List<Tuple2<String,String>>  movielist=movierdd.mapToPair(x->{
            String[] arraystr=x.split("::");
            return new Tuple2<>(arraystr[0],arraystr[1]);
        }).collect();

        //mapmovies
        Map<String,String> mapmovies=new HashMap<>();
        movielist.stream().forEach(x->mapmovies.put(x._1(),x._2()));

        //
        Map<Integer,String> maptopmovies=new HashMap<>();
        topMovies.stream().forEach(x->maptopmovies.put(x._1(),mapmovies.getOrDefault(x._2(), null)));

        for(Map.Entry<Integer,String> item :maptopmovies.entrySet()){
            System.out.println(item.getKey()+"   "+item.getValue());
        }



    }
}
