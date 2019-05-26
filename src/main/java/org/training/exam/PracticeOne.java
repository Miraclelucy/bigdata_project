package org.training.exam;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by lushiqin on 2019-05-26.
 * 请利用 RDD API 编写代码片段实现以下功能：
 1）请输出该数据集包含的所有不同国家的名称（用到 country 一列，第21列）
 2）请输出该数据集中包含的中国电影的数目（用到 country 一列）
 3）请输出最受关注的三部中国电影的电影名称、导演以及放映时间
 *
 */
public class PracticeOne {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[1]").getOrCreate();
        // 将文件读成RDD
        JavaRDD<String> rdd = spark.sparkContext().textFile("data/movie_metadata.csv", 10).toJavaRDD();
        // 过滤第一行，分隔csv
        JavaRDD<String[]> movieRdd = rdd.filter(x -> (!x.startsWith("color,director_name"))).map(x ->
                x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));

        spark.sparkContext().setLogLevel("WARN");
        // 1 请输出该数据集包含的所有不同国家的名称（用到 country 一列，第21列）
        JavaRDD<String> countryrdd=movieRdd.map(x->x[20]).distinct();
        countryrdd.foreach(x->{
            System.out.println(x);
        });


        //2 请输出该数据集中包含的中国电影的数目（用到 country 一列）
        JavaRDD<String[]> chinardd=movieRdd.filter(x->x[20].equalsIgnoreCase("china"));
        long chinamovienum= chinardd.count();
        System.out.println("china moive nums :"+chinamovienum);

        //3 请输出最受关注的三部中国电影的电影名称、导演以及放映时间
        //  用到 movie_title第12列 、director_name第2列、num_voted_users第13列、country
        //  以及 title_year第24列 共五列）
        //JavaRDD<Tuple2<Long,Tuple3<String,String,String>>> topmovierdd=

        chinardd
        .mapToPair(x->new Tuple2<>(Integer.parseInt(x[12]),new Tuple3<>(x[11],x[1],x[23])))
        .sortByKey(false)
        .take(10)
        .stream()
        .forEach(x->{
            System.out.println("top 3 movie:"+x);
        });

    }
}
