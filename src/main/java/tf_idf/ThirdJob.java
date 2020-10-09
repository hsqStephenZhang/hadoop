package tf_idf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdJob {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");
        conf.set("mapreduce.app-submission.coress-platform", "true");
        conf.set("mapreduce.framework.name", "local");

        FileSystem fs = FileSystem.get(conf);

        //采用服务器环境测试

        Job job = Job.getInstance(conf);
        job.setJarByClass(ThirdJob.class);

        job.setMapperClass(ThirdMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ThirdReduce.class);
        //把常用文件数据加载到内存中，提高获取速度
        job.addCacheFile(new Path("/testOutput/first/part-r-00003").toUri());//文件总数
        job.addCacheFile(new Path("/testOutput/second/part-r-00000").toUri());//含有特定词的文件数

        Path inPath = new Path("/testOutput/first/");
        FileInputFormat.addInputPath(job, inPath);

        Path outPath = new Path("/testOutput/third");
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        //提交job，等待完成！
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("Job success!");
        }
    }
}


/**
 * 对每个文件中每个词进行TF-IDF计算
 * @author lenovo
 *
 */
class ThirdMap extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, Integer> df = new HashMap<String, Integer>();//包含特定词的文件数
    private Map<String, Integer> count = new HashMap<String, Integer>();//文件总数

    /**
     * 在map函数执行前执行
     * 从内存中将数据取出，置于map容器中
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取内存中的文件
        URI[] cacheFiles = context.getCacheFiles();
        //文件内容加载到容器内
        if (cacheFiles != null) {
            for (int i = 0; i < cacheFiles.length; i++) {
                URI uri = cacheFiles[i];
                Path path = new Path(uri.getPath());
                BufferedReader buffer = new BufferedReader(new FileReader(path.getName()));

                if (uri.getPath().endsWith("part-r-00003")) {//文件总数
                    //读取内容
//                    System.out.println("path:"+uri.getPath()+"  filename:"+path.getName());
                    String line = buffer.readLine();
                    if (line.startsWith("count")) {
                        String[] split = line.split("\t");
                        count.put(split[0], Integer.valueOf(split[1].trim()));
//                        System.out.println(Arrays.toString(split));
                    }
                    buffer.close();
                }else if(uri.getPath().endsWith("part-r-00000")){//含有特定词的文件数
                    //读取内容
                    String line ;
                    while((line = buffer.readLine()) != null){
                        String[] split = line.split("\t");
                        df.put(split[0], Integer.parseInt(split[1].trim()));
//                        System.out.println(Arrays.toString(split));
                    }
                    buffer.close();
                }

            }
        }

    }

    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        System.out.println(fileSplit);
        if (!fileSplit.getPath().getName().contains("part-r-00003")) {
            String[] split = value.toString().trim().split("\t");
            System.out.println(Arrays.toString(split));
            if (split.length>=2) {
                int TF = Integer.parseInt(split[1]);//词频，每个词在所在文件中出现的次数

                String[] ss = split[0].split("_");
                if (ss.length >=2) {
                    String word = ss[0];
                    String id = ss[1];

                    double TF_IDF = TF * Math.log(count.get("count")/df.get(word));

                    System.out.println(TF_IDF);
                    NumberFormat f = NumberFormat.getInstance();
                    f.setMaximumFractionDigits(5);

                    context.write(new Text(id), new Text(word+"_"+f.format(TF_IDF)));
                }
            }
        }
    }
}




class ThirdReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> itr,Context context) throws IOException,
            InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text text : itr) {
            sb.append(text.toString()+"\t");
        }
        context.write(key, new Text(sb.toString()));
    }
}

