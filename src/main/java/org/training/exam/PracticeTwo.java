package org.training.exam;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;


/**
 * Created by lushiqin on 2019-05-26.
 * 请采用 DataFrame/Dataset API 实现：
 1）请输出接收到希拉里邮件最多的前三个人（用到 EmailReceivers.csv 文件，只需输出用户 ID）
 2）请输出希拉里收发邮件最多的前三天
 3）请输出希拉里最喜欢使用的十个单词
 *
 */
public class PracticeTwo {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> receivers = spark.read().option("header","true").csv("emails/EmailReceivers.csv");
        Dataset<Row> emails = spark.read().option("header","true").csv("emails/Emails.csv");


        //1 请输出接收到希拉里邮件最多的前三个人
        receivers.where("PersonId<>80")// 排除希拉希自己
                 .groupBy("PersonId")
                 .agg(countDistinct("Id").as("sum"))
                 .orderBy(col("sum").desc())
                 .limit(3).select("PersonId").show();

        //2 输出希拉里收发邮件最多的前三天  MetadataDateSent,MetadataDateReleased
        emails.groupBy("MetadataDateSent")
                .agg(countDistinct("Id").as("sum"))
                .orderBy(col("sum").desc())
                .take(3);


        //3 请输出希拉里最喜欢使用的十个单词
        //Dataset<Row> content =  emails.agg("ExtractedCc");




    }

}
