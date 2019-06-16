package org.training.flink;

/**
 * Created by lushiqin on 2019-06-15.
 */
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<String> text = env.socketTextStream(args[0], Integer.parseInt(args[1]), '\n');

        DataStream<Long> count = text
                .flatMap(new FlatMapIterator<String, String>() {
                    @Override
                    public Iterator<String> flatMap(String value) throws Exception {
                        return Iterators.forArray(value.split("\\s"));
                    }
                })
                .map (s -> {
                    try { return Long.parseLong(s); } catch (NumberFormatException ne) { return 0L;}
                })
                .timeWindowAll(Time.seconds(10))
                .reduce((x,y) -> x + y);

        count.print().setParallelism(1);

        // execute program
        env.execute("Flink Streaming Word Count");
    }
}
