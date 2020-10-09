package tf_idf;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");
        conf.set("mapreduce.app-submission.coress-platform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondJob.class);

        job.setMapperClass(SecondMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(SecondReduce.class);

//		job.setNumReduceTasks(4);
        job.setReducerClass(SecondReduce.class);

        Path inPath = new Path("/testOutput/first");
        FileInputFormat.addInputPath(job, inPath);

        Path outPath = new Path("/testOutput/second");
        FileSystem fs = FileSystem.get(conf);
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

class SecondMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 对除了最后一个分区文件内的每行数据进行统计
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //过滤掉不要处理的文件
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        if (!fileSplit.getPath().getName().contains("part-r-00003")) {
            String[] words = value.toString().trim().split("\t");
            if (words.length>=2) {
                String ss = words[0].trim();
                String[] split = ss.split("_");
                if (split.length>=2) {
                    String w = split[0];
                    context.write(new Text(w), new IntWritable(1));
                }
            }
        }
    }
}


class SecondReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text w, Iterable<IntWritable> itr,Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : itr) {
            sum +=i.get();
        }
        context.write(w, new IntWritable(sum));
    }
}


