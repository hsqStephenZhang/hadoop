package tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

public class FirstJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");

        conf.set("mapreduce.app-submission.coress-platform", "true");
        conf.set("mapreduce.framework.name", "local");

        FileSystem fs = FileSystem.get(conf);
        //实例化job
        Job job = Job.getInstance(conf);
        //job入口
        job.setJarByClass(FirstJob.class);
        //设置map相关信息，包括自定义的map类，map输出key的类型，输出value的类型
        job.setMapperClass(FirstMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置job的shuffle过程操作信息，包括分区，排序，分组
        job.setPartitionerClass(FirstPartition.class);
        job.setCombinerClass(FirstReduce.class);
//			job.setSortComparatorClass();
//			job.setGroupingComparatorClass();
        //设置reduce相关信息，包括reduce任务个数，自定义的reduce类
        job.setNumReduceTasks(4);
        job.setReducerClass(FirstReduce.class);
        //设置要处理的文件
        Path inPath = new Path("/testInput");
        FileInputFormat.addInputPath(job, inPath);
        //设置最终结果保存的路径
        Path outPath = new Path("/testOutput/first");
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

class FirstMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 对读取的每一行进行分词
     * 每行处理后输出<词,1>的数据和<count,1>
     */
    @Override
    protected void map(LongWritable key, Text line,Context context)
            throws IOException, InterruptedException {

        String[] words = line.toString().trim().split("\t");
        if (words.length>=2) {
            String id = words[0].trim();
            System.out.println("id:"+id);
            String content = words[1].trim();
            System.out.println("content:"+content);

            StringReader sr = new StringReader(content);
            IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
            Lexeme word;
            while ((word = ikSegmenter.next())!= null) {
                String w = word.getLexemeText();
                System.out.println(w);
                context.write(new Text(w+"_"+id), new IntWritable(1));
            }
            context.write(new Text("count"),new IntWritable(1));
        }else{
            System.out.println(line.toString()+"------------------");
        }
    }
}

class FirstPartition extends HashPartitioner<Text, IntWritable> {

    /**
     * 返回值作为分区号，即分区文件的序号
     * 分区个数与reduce任务个数相同
     * 假设有4个reduce任务，则有4个分区，分区文件序号从0开始。
     */
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if (key.equals(new Text("count"))) {
            return 3;//将键为count的数据放在序号为3的分区文件中
        } else {//其他数据对键进行hash取模，放到前三个分区文件
            return super.getPartition(key, value, numReduceTasks - 1);
        }
    }
}


class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text word, Iterable<IntWritable> itr, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : itr) {
            sum += i.get();
        }
        context.write(new Text(word), new IntWritable(sum));
    }
}