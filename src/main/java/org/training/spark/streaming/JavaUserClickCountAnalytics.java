package org.training.spark.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.training.spark.streaming.utils.ConnectionPool;
import org.training.spark.streaming.utils.KafkaRedisConfig;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by lushiqin on 2019-05-24.
 * 用户点击事件实时分析,数据入库到mysql
 */
public class JavaUserClickCountAnalytics {
    public static void main(String[] args) throws  InterruptedException {
        //1 kafka的设置参数
        String[] topics = KafkaRedisConfig.KAFKA_USER_TOPIC.split(",");
        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2 初始化JavaStreamContext
        SparkConf conf=new SparkConf().setMaster("local[2]")
                .setAppName("JavaUserClickCountAnalytics");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(60));

        //3 spark steaming从kafka去读数据 产生一个JavaPairInputDStream
        JavaPairInputDStream<String,String> inputDStream=KafkaUtils.createDirectStream(jsc,String.class,String.class,
                StringDecoder.class,StringDecoder.class,
                props,
                new HashSet<String>(Arrays.asList(topics))
                );


        //4 产生一个JavaDStream 将每一行记录转换为json对象
        JavaDStream<JSONObject>  dStream = inputDStream.map(line-> JSON.parseObject(line._2()));

        //5 产生一个JavaPairDStream 抽取每一行中的uid,click_count,并做归约处理
        JavaPairDStream<String, Long> usersDStream=dStream.mapToPair(x->
                new Tuple2<>(x.getString("uid"), x.getLong("click_count"))).reduceByKey((x,y)->x+y);

        //6 处理JavaPairDStream，数据入库到mysql
        usersDStream.foreachRDD(rdd->{
            rdd.foreachPartition(record->{
                Connection conn=ConnectionPool.getConnection();
                record.forEachRemaining(x->{
                    try {
                        Statement st = conn.createStatement();
                        String sql="insert into streaming_word_count(uid,count) values('" + x._1 + "'," + x._2 + ")";
                        st.execute(sql);
                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                });
                ConnectionPool.returnConnection(conn); // 归还conn


                /*数据入库到redis
                Jedis jedis = JavaRedisClient.get().getResource();
                    while(partitionOfRecords.hasNext()) {
                        try {
                            Tuple2<String, Long> pair = partitionOfRecords.next();
                            String uid = pair._1 ();
                            long clickCount = pair._2();
                            jedis.hincrBy(clickHashKey, uid, clickCount);
                            System.out.println("Update uid " + uid + " to " + clickCount);
                        } catch(Exception e) {
                            System.out.println("error:" + e);
                        }
                    }
                 jedis.close();
                * */
            });
        });

        jsc.start();
        jsc.awaitTermination();

    }
}
