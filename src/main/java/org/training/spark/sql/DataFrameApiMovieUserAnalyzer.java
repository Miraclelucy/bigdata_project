package org.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.*;

/**
 * Created by lushiqin on 2019-05-26.
 * 练习DataFrame的Api
 *
 */
public class DataFrameApiMovieUserAnalyzer {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("DataFrameApiMovieUserAnalyzer")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "10")
                .config("spark.sql.autoBroadcastJoinThreshold",20485760)
                .getOrCreate();

        String inputPath = "data/ml-1m/users.json";
        if(args.length > 0) {
            inputPath = args[0];
        }

       // Dataset<Row> ds=ss.read().

        Dataset<Row> userDF =
                spark.read().format("json").load("/tmp/users.json");
        userDF.show(4);
        userDF.limit(2).toJSON().foreach((String s) -> System.out.println(s));
        userDF.printSchema();
        spark.stop();


        userDF.withColumn("age2", col("age").plus(1));
        userDF.collect();
        userDF.first();
        userDF.take(2);
        userDF.head(2);

        userDF.select("userID", "age").show();
        userDF.selectExpr("userID", "ceil(age/10) as newAge").show(2);
        userDF.select(max("age"), min("age"), avg("age")).show();
        userDF.filter(col("age").gt(30)).show(2);
        userDF.filter("age > 30 and occupation = 10").show();
        userDF.select("userID", "age").filter("age > 30").show(2);
        userDF.filter("age > 30").select("userID", "age").show(2);

        userDF.groupBy("age").count().show();
        userDF.groupBy("age").agg(count("gender"),countDistinct("occupation")).show();


    }
}
