package org.training.spark.streaming.hotel;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.receiver.RateLimiter;
import org.training.spark.streaming.utils.KafkaRedisConfig;
import org.training.spark.streaming.utils.LogUtil;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;


/**
 * Created by lushiqin on 2019-06-02.
 */
public class JavaKafkaHotelInfoProducer {
    private RateLimiter limiter;
    private String topic;
    private  Producer<String, String> producer;

    public JavaKafkaHotelInfoProducer(int maxRatePerSecond) {
        String topic = KafkaRedisConfig.KAFKA_USER_TOPIC;
        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    public void replayLog(String logFile) {
        //1 每一行的数据依次读出来
        try (Stream<String> stream = Files.lines(Paths.get(logFile))) {
            Iterator<String> itr = stream.iterator();
            while (itr.hasNext()) {
                //limiter.acquire();
                //构造数据，将log日志文件中的时间戳替换为当前时间
                String json = LogUtil.resetSecond(itr.next(), System.currentTimeMillis()/1000);
                System.out.println(json);
                //
                producer.send(new KeyedMessage<String, String>(topic, json));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: JavaKafkaLogProducer <maxRatePerSecond> <logFilePath>");
            System.exit(-1);
        }
        int maxRate = Integer.parseInt(args[0]);
        JavaKafkaHotelInfoProducer producer = new JavaKafkaHotelInfoProducer(maxRate);
        String logpath="/app/sparkdata/hotel/ord_train.csv";
        producer.replayLog(logpath);


    }


}
