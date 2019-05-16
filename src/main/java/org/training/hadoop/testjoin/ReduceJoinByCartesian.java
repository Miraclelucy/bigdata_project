package org.training.hadoop.testjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lushiqin on 20190418.
 * 适用场景：两个表连接
 * 实现方式：笛卡尔积
 */
public class ReduceJoinByCartesian {

    public static void main(String[] args)  throws  Exception {
        Configuration conf = new Configuration();
        //conf.set
        Job job = Job.getInstance(conf,"ReduceJoinByCartesian");

        Path inputPath= new Path(args[0]);
        Path outputPath= new Path(args[1]);

        FileSystem fs=outputPath.getFileSystem(conf);
        if(fs.isDirectory(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        job.setJarByClass(ReduceJoinByCartesian.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //指定mapper输出的k v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出kv类型（reduce输出类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }


    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Text joinkey=new Text();
        private Text combinekey=new Text();

        @Override
        protected void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            String pathName=((FileSplit)context.getInputSplit()).getPath().toString();
            if(pathName.endsWith("record.txt")){
                String[] arr= StringUtils.split(value.toString()," ");
                if(arr.length!=3){
                    return;
                }
                joinkey.set(arr[0]);
                combinekey.set("record.txt"+arr[1]+" "+arr[2]);
                System.out.println("record.txt"+arr[1]+" "+arr[2]);
            }else if(pathName.endsWith("station.txt")){
                String[] arr= StringUtils.split(value.toString()," ");
                if(arr.length!=2){
                    return;
                }
                joinkey.set(arr[0]);
                combinekey.set("station.txt"+arr[1]);
                System.out.println("station.txt"+arr[1]);
            }
            context.write(joinkey,combinekey);
        }


    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> leftTable=new ArrayList<String>();
        private List<String> rigthTable=new ArrayList<String>();
        private Text result=new Text();
        @Override
        protected void reduce(Text key,Iterable <Text> values,Context context) throws IOException,InterruptedException{
            leftTable.clear();
            rigthTable.clear();
            for(Text value:values){
                if(value.toString().startsWith("record.txt")){
                    leftTable.add(value.toString().replaceFirst("record.txt",""));
                }else if(value.toString().startsWith("station.txt")){
                    rigthTable.add(value.toString().replaceFirst("station.txt",""));
                }
            }

            for(String ritem:rigthTable){
                for(String litem :leftTable){
                    System.out.println(ritem+" "+litem);
                    result.set(ritem+" "+litem);
                    context.write(key,result);
                }
            }
        }
    }




}
