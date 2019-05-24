package org.training.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Created by lushiqin on 5/20/19.
 * spark sql for word counting
 */
public class Java8WordCountV2 {

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder()
                .appName("Java8WordCountV2")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "10") //Reduce task 数目,默认是200
                .config("spark.sql.autoBroadcastJoinThreshold",20485760) //广播小表大小,默认10MB
                .config("spark.sql.files.maxPartitionBytes",20485760) //读数据时每个Partition大小,默认128MB
                .enableHiveSupport()
                .getOrCreate();

        String inputPath = "data/textfile/Hamlet.txt";
        if(args.length > 0) {
            inputPath = args[0];
        }

        //1 create dataset
        Dataset<String> ds=ss.read().textFile(inputPath);

        //2 transform dataset
        Dataset<String> words=ds.flatMap(x-> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        //3 transform dataset to DataFrame
        Dataset<Row> result=words
                .toDF("word")  //对列重命名成“word”，toDF是transfornamation
                .groupBy("word")
                .count(); //Returns the number of rows in the Dataset.
        result.show();
        //4 create temp table
        words.createOrReplaceTempView("words");

        Dataset<Row>  wordsresult=ss.sql("select value, count(*) as count from words group by value");

        wordsresult.write().mode("overwrite").parquet("/tmp/parquet");
        wordsresult.write().mode("overwrite").csv("/tmp/csv");


    }
}
