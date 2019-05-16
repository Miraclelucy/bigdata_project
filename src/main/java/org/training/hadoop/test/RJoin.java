package org.training.hadoop.test;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.derby.iapi.store.access.conglomerate.Sort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  @author lushiqin 20190327
 *  MR实现Join逻辑
 *
 * */
public class RJoin {

    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{

        InfoBean bean = new InfoBean();
        Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //由于读取文件后，获取的内容不好区分，是订单文件还是产品文件，我们可以通过分区来获取文件名来去人，我的订单文件名是包含order的
            FileSplit  split = (FileSplit) context.getInputSplit();
            //获取文件名称
            String fileName = split.getPath().getName();
            //通过空格分割
            String[] strs = value.toString().split(" ");
            String flag = "";//标记
            String pId = "";//产品id
            if(fileName.contains("order")){
                //处理订单信息-id 日期 产品id 数量
                //订单id
                int orderId = Integer.parseInt(strs[0]);
                String dateString = strs[1];
                //产品id
                pId = strs[2];
                int amount = Integer.parseInt(strs[3]);
                flag = "0";
                bean.set(orderId, dateString, pId, amount, "", 0, 0, flag);
            }else{
                //处理产品信息-产品id  产品名称  分类id  价格
                pId  = strs[0];
                String pName = strs[1];
                int categoryId = Integer.parseInt(strs[2]);
                float price = Float.parseFloat(strs[3]);
                flag = "1";
                bean.set(0, "", pId, 0, pName, categoryId, price, flag);
            }
            text.set(pId);
            context.write(text, bean);
        }
    }

    static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<InfoBean> infoBeans,
                              Context context) throws IOException, InterruptedException {
            InfoBean pBean = new InfoBean();
            List<InfoBean> list = new ArrayList<>();
            for (InfoBean infoBean : infoBeans) {
                if("1".equals(infoBean.getFlag())){//flag 0是订单信息  1是产品信息
                    try {
                        BeanUtils.copyProperties(pBean, infoBean);//数据必须进行拷贝，不可直接赋值
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else{
                    //处理订单信息
                    InfoBean orderBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(orderBean, infoBean);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    //由于订单和产品的关系是多对一的关系，所有订单要用list临时存放起来
                    list.add(orderBean);
                }

            }
            for (InfoBean orderBean : list) {
                orderBean.setCategoryId(pBean.getCategoryId());
                orderBean.setpName(pBean.getpName());
                orderBean.setPrice(pBean.getPrice());
                //写出
                context.write(orderBean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		/*conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resoucemanger.hostname", "hadoop01");*/
        Job job = Job.getInstance(conf);

        job.setJarByClass(RJoin.class);

       // job.setJarByClass(Sort.class);

        //指定本业务job要使用的业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);



        //指定mapper输出的k v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        //指定最终输出kv类型（reduce输出类型）
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定job的输入文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所有的java类所在 的jar包，提交给yarn去运行
        //job.submit();无结果返回，建议不使用它
        boolean res = job.waitForCompletion(true);

       System.exit(res?0:1);


    }


}
