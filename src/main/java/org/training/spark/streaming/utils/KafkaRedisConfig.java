package org.training.spark.streaming.utils;

/**
 * Created by lushiqin on 2019-05-24.
 */
public class KafkaRedisConfig {
    public static String REDIS_SERVER = "master-129";
    public static int REDIS_PORT = 6379;

    public static String KAFKA_SERVER = "master-129";
    public static String KAFKA_ADDR = KAFKA_SERVER + ":9092";
    public static String KAFKA_USER_TOPIC = "user_events";

    public static String ZOOKEEPER_SERVER = "master-129:2181";
    public static String ZOOKEEPER_PATH = "/offsets";

}
