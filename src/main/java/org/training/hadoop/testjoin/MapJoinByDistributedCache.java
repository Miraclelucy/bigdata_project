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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

/**
 * Created by lushiqin on 20190420.
 * 使用场景：一张表十分小，一张表很大。
 */
public class MapJoinByDistributedCache {

    public static void main(String[] args)  throws  Exception {
        Configuration conf = new Configuration();
        //conf.set
        Job job = Job.getInstance(conf,"ReduceJoinByDistributedCache");

        //DistributedCache.addCacheFile(new Path(args[2]).toUri(),conf);//small table -station.txt
        job.addCacheFile(new Path(args[2]).toUri());//small table -station.txt
        job.setJarByClass(MapJoinByDistributedCache.class);

        Path inputPath= new Path(args[0]);//big table -record.txt
        Path outputPath= new Path(args[1]);// output path

        FileSystem fs=outputPath.getFileSystem(conf);
        if(fs.isDirectory(outputPath)){
            fs.delete(outputPath,true);
        }

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setMapperClass(MyMapper.class);
        //job.setReducerClass(MyReducer.class);

        //指定mapper输出的k v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出kv类型（reduce输出类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }


    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Hashtable<String,String> hashtable=new Hashtable<String,String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br;
            String infoaddr="";
            URI[] files = context.getCacheFiles();
            //URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());

            for(URI file :files){
                Path path = new Path(file);
                br=new BufferedReader(new FileReader(path.getName()));
                while(null!=(infoaddr=br.readLine())){
                    String[] arrs=StringUtils.split(infoaddr," ");
                    if(arrs!=null) {
                        String stationid = arrs[0];
                        String stationname = arrs[1];
                        hashtable.put(stationid,stationname);
                    }
                }
            }
        }
        @Override
        protected void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            String[] arr= StringUtils.split(value.toString()," ");
            if(arr.length==3){
                String stationname=hashtable.get(arr[0]);
                context.write(new Text(stationname),value);
            }
        }
    }

}
