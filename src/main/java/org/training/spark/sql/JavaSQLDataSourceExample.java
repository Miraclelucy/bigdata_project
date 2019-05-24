package org.training.spark.sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Arrays;
/**
 * Created by lushiqin on 5/21/19.
 *
 *
 */
public class JavaSQLDataSourceExample {

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder()
                .appName("JavaSQLDataSourceExample")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "10")
                .config("spark.sql.autoBroadcastJoinThreshold",20485760)
                .getOrCreate();

        ss.sparkContext().setLogLevel("WARN");

        String inputPath = "data/ml-1m/users.json";
        if(args.length > 0) {
            inputPath = args[0];
        }

        //1 read json data and save to parquet
        Dataset<Row> ds_json =ss.read().json(inputPath);
        long countnum=ds_json.count();
        System.out.println("ds_json:countnum:"+countnum);
        ds_json.write().mode("overwrite").parquet("data/tmp/parquet");

        //2 read parquet data and count data
        Dataset<Row> ds_parquet =ss.read().parquet("data/tmp/parquet");
        long countnums=ds_parquet.count();
        System.out.println("ds_parquet:countnums:"+countnums);

        //3
        Dataset<Row> ds = ss.read().parquet("data/ml-1m/ratings.parquet");
        ds.write().mode("overwrite").csv("dataoutput/csv");
        ds.write().mode("overwrite").json("dataoutput/json");
        ds.write().mode("overwrite").parquet("dataoutput/parquet");
        ds.write().mode("overwrite").orc("dataoutput/orc");

    }
}
