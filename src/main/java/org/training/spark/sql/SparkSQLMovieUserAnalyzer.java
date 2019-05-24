package org.training.spark.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by lushiqin on 5/21/19.
 * 用户电影数据分析
 */
public class SparkSQLMovieUserAnalyzer {
    public static void main(String[] args)  throws AnalysisException{
        SparkSession ss=SparkSession.builder()
                .appName("SparkSQLMovieUserAnalyzer")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "10")
                .config("spark.sql.autoBroadcastJoinThreshold",20485760)
                .getOrCreate();

        ss.sparkContext().setLogLevel("WARN");
        String usersPath = "data/ml-1m/users.parquet";
        String ratingsPath ="data/ml-1m/ratings.parquet";
        String moviesPath ="data/ml-1m/movies.parquet";

        Dataset<Row> userds=ss.read().parquet(usersPath);
        Dataset<Row> ratingsds=ss.read().parquet(ratingsPath);
        Dataset<Row> moviesds=ss.read().parquet(moviesPath);


        userds.createGlobalTempView("users");

        //统计各个职业的男女人数
        Dataset<Row>  genderAndoccupation =ss.sql(
                "select count(1),occupation ,gender from global_temp.users " +
                        "group by occupation,gender  order by occupation");
        //genderAndoccupation.show(200);


        //统计各个职业的最小年龄和最大年龄
        Dataset<Row>  ageAndoccupation =ss.sql(
                " select count(1),occupation , " +
                        "first_value(age)  over ( partition by occupation order by age) as minage ," +
                        "last_value(age)  over ( partition by occupation order by age) as maxage ," +
                        "age from global_temp.users " +
                        "group by occupation,age  order by occupation,age");
        //ageAndoccupation.show(200);


        //统计各个年龄段的人口数 [1, 22), [22, 30), [30, 45), [45, 60)
        Dataset<Row>  agePerson =ss.sql(
                "select count(1) from global_temp.users " +
                        "where age between 1 and 22");
        agePerson.show(200);


        //创建电影表和打分表的临时表, 并用Spark SQL查询看过Phantasm四部曲（ Phantasm I～ IV) 的总人数
        ratingsds.createGlobalTempView("ratings");
        moviesds.createGlobalTempView("movies");

        Dataset<Row>  whoWatchPhantasm =ss.sql(
                "select t.userid  from (select count(1) as count,userid from global_temp.ratings a " +
                        "where a.movieid in ('2901','3837','3838','3839') group by userid ) t  where count=4");
        whoWatchPhantasm.show(200);



    }
}
