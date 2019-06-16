import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/*
问题描述：
有一个目录（里面是多个文件，如1000个文件，001.txt-999.txt，都是大文件200、300M的，或者更大的），
要在每个文件内加上部分数据（加数据的逻辑是在单个文件处理的），
但是要求处理后的文件内容不能重新洗牌，
最后输出的结果还是那1000个文件，001.txt-999.txt

原方案：
之前为了获取文件名称，用了mapPartitionsWithInputSplit，
然后以为一个文件就是一个InputSplit，但是后面用大文件测试才知道会被分成多个片。
测试了这种方案，就是因为一个文件被分了多个片了，所以这样保存文件的时候，
只保留了一个文件的某个分片的数据，数据丢失了

改进后的方案：
1、读取hdfs目录下的文件名，通过文件名一一对应需要所在行插入的watermark;
2、文件名map操作建立对应的RDD，并zipWithIndex对应每一行号
3、union所有的RDD，对RDD进行mapPartitions操作对指定的文件和对应行插入watermark
4、重写MultipleTextOutputFormat，根据文件名保存partition的数据

注意点：
首先第一点，N个文件就用textFile读取n次，不会这么处理的呢
其次，判断空文件，不需要用count，take就可以了
sparkContext每个线程一个sparkcontext，sc肯定不能公用，一个线程一个

*/

object testMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","hadoop-twq")
    val conf=new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .setAppName("testMain")
      .setMaster("local[*]")
    val sc=new SparkContext(conf)

    val dest = "hdfs://master:9999/user/hadoop-twq/input/filelist"

    val configuration = new Configuration
    val fileSystem = FileSystem.get(URI.create(dest), configuration)
    val listStatus = fileSystem.listStatus(new Path(dest))
    val paths:Array[String] = FileUtil.stat2Paths(listStatus).map(path=>dest+"/"+path.getName)
    val insertMap:Map[String,(Int,String)] = FileUtil.stat2Paths(listStatus).map(path=>(path.getName,(2,"insertContent"))).toMap
    fileSystem.close()

    val insertMapbroadcast = sc.broadcast(insertMap)

    val inputRDDs:Array[RDD[(String,(Long,String))]] = paths.map { path =>
      val hdfsRDD = sc.newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

      val pathRDD = hdfsRDD.asInstanceOf[NewHadoopRDD[LongWritable, Text]].mapPartitionsWithInputSplit {
        case (inputsplit, iter) =>
          val fileName = inputsplit.asInstanceOf[FileSplit].getPath.getName
          iter.map { case (_, text) => (fileName, text.toString) }
      }
      pathRDD.zipWithIndex().map{case((fileName,text),index)=>(fileName,(index,text))}
    }

    val unionRDD = sc.union(inputRDDs)

    val insertRDD = unionRDD.mapPartitions{iter =>
      val insertMapValue = insertMapbroadcast.value
      iter.map{case(fileName,(index,text))=>
        val (indexNeed,textAppend) = insertMapValue(fileName)
        if(indexNeed == index) (fileName,text+textAppend)
        else (fileName,text)
      }
    }

    insertRDD.saveAsHadoopFile(
      "hdfs://master:9999/user/hadoop-twq/input/filelist/output",
      classOf[String],
      classOf[String],
      classOf[PairRDDMultipleTextOutputFormat])

  }
}

/**继承类重写方法*/
class PairRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  //1)文件名：根据key和value自定义输出文件名。
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
    val fileNamePrefix=key.asInstanceOf[String]
    val fileName=fileNamePrefix+"-"+name
    fileName
  }

  //2)文件内容：默认同时输出key和value。这里指定不输出key。
  override def generateActualKey(key: Any, value: Any): String = {
    null
  }
}



