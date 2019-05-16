package org.training.hadoop.testjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by lushiqin on 20190401.
 * 适用场景：两个表连接
 * 实现方式：二次排序
 */
public class ReduceJoinBySecondarySort {


    public  static void main(String args[]) throws  Exception{
        Configuration conf = new Configuration();
        //conf.set
        Job job = Job.getInstance(conf,"ReduceJoinBySecondarySort");

        Path stationInputPath= new Path(args[0]);
        Path recordInputPath= new Path(args[1]);
        Path outputPath= new Path(args[2]);

        MultipleInputs.addInputPath(job,recordInputPath,TextInputFormat.class,JoinRecordMapper.class);
        MultipleInputs.addInputPath(job,stationInputPath,TextInputFormat.class,JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job,outputPath);
        job.setReducerClass(JoinReducer.class);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setNumReduceTasks(2);
        //指定mapper输出的k v类型
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出kv类型（reduce输出类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }

    /***
     * define mapper
     */
    static class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text>{
        @Override

        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            String[] arr= StringUtils.split(value.toString()," ");
            System.out.println("map---stationId--"+arr[0]);
            System.out.println("map---stationName--"+arr[1]);
            if(arr.length==2){
                //stationId,stationName
                context.write(new TextPair(arr[0],"0"),new Text(arr[1]));

            }
        }


    }

    /***
     * define mapper
     */
    static class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text>{
        @Override

        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            String[] arr=StringUtils.split(value.toString()," ");
            System.out.println("map2---stationId--"+arr[0]);
            System.out.println("map2---temperature--"+arr[1]);
            System.out.println("map2---timestamp--"+arr[2]);
            if(arr.length==3){
                //stationId,temperature,timestamp
                context.write(new TextPair(arr[0],"1"),new Text(arr[1]+"\t"+arr[2]));

            }
        }


    }



    /***
     * define reducer
     */
    static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
        @Override

        protected void reduce(TextPair key,Iterable <Text> values,Context context) throws IOException,InterruptedException{
            Iterator<Text> iter=values.iterator();
            Text stationName=new Text(iter.next()); //这里为什么？迭代器中取出来得第一个是stationName
            System.out.println("reduce---stationName--"+stationName.toString());

            while(iter.hasNext()){
                Text record =iter.next(); //这里为什么？迭代器中取出来得第二个是record
                Text outValue=new Text(stationName.toString()+"\t"+record.toString());
                System.out.println("reduce---record--"+record.toString());

                context.write(key.getFirst(),outValue);

            }
        }


    }

    /***
     * define Partitionerd
     */
    static class KeyPartitioner extends Partitioner<TextPair, Text> {
        public int getPartition(TextPair key, Text value,int numPartitions){
            System.out.println("key.getFirst().hashCode():"+key.getFirst().hashCode());
            System.out.println("numPartitions:"+numPartitions);
            System.out.println("Partitioner:"+(key.getFirst().hashCode()&Integer.MAX_VALUE)%numPartitions);
            return (key.getFirst().hashCode()&Integer.MAX_VALUE)%numPartitions;

        }
    }


    /***
     * define GroupingComparator
     */
    static class GroupingComparator extends WritableComparator {
        protected GroupingComparator(){
            super(TextPair.class,true);
        }

        public int compare(WritableComparable w1,WritableComparable w2){
            TextPair ip1=(TextPair)w1;
            TextPair ip2=(TextPair)w2;
            Text l=ip1.getFirst();
            Text r=ip2.getFirst();
            System.out.print("GroupingComparator:");
            System.out.println(l.compareTo(r));
            return l.compareTo(r);
        }

    }







}
