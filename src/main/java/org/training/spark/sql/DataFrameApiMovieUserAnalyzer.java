package org.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by lushiqin on 2019-05-22.
 * 练习DataFrame的Api
 *
 */
public class DataFrameApiMovieUserAnalyzer {

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder()
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


    }
}
