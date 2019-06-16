package org.training.flink;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

    public static void main(String[] args) throws Exception {

        final long numSamples = args.length > 0 ? Long.parseLong(args[0]) : 1000000;

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // count how many of the samples would randomly fall into
        // the unit circle
        DataSet<Long> count =
                env.generateSequence(1, numSamples)
                        .map(new MapFunction<Long, Long>() {
                            @Override
                            public Long map(Long aLong) throws Exception {
                                double x = Math.random();
                                double y = Math.random();
                                return (x * x + y * y) < 1 ? 1L : 0L;
                            }
                        })
                        .reduce((x,y) -> x + y);

        long theCount = count.collect().get(0);

        System.out.println("We estimate Pi to be: " + (theCount * 4.0 / numSamples));

    }
}