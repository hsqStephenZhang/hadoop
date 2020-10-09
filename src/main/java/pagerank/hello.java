package pagerank;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class hello {

    /*map过程*/
    public static class lxnmapper extends Mapper<Object,Text,Text,Text>{
        private String id;
        private float pr;
        private int count;
        private float average_pr;
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            StringTokenizer str = new StringTokenizer(value.toString());//对value进行解析
            id =str.nextToken();//id为解析的第一个词，代表当前网页
            pr = Float.parseFloat(str.nextToken());//pr为解析的第二个词，转换为float类型，代表PageRank值
            count = str.countTokens();//count为剩余词的个数，代表当前网页的出链网页个数
            average_pr = pr/count;//求出当前网页对出链网页的贡献值
            String linkids ="&";//下面是输出的两类，分别有'@'和'&'区分
            while(str.hasMoreTokens()){
                String linkid = str.nextToken();
                context.write(new Text(linkid),new Text("@"+average_pr));//输出的是<出链网页，获得的贡献值>
                linkids +=" "+ linkid;
            }
            context.write(new Text(id), new Text(linkids));//输出的是<当前网页，所有出链网页>
        }
    }

    /*reduce过程*/
    public static class lxnreduce extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
            String lianjie = "";
            float pr = 0;
            /*对values中的每一个val进行分析，通过其第一个字符是'@'还是'&'进行判断
            通过这个循环，可以 求出当前网页获得的贡献值之和，也即是新的PageRank值；同时求出当前
            网页的所有出链网页 */
            for(Text val:values){
                if(val.toString().substring(0,1).equals("@")){
                    pr += Float.parseFloat(val.toString().substring(1));
                }
                else if(val.toString().substring(0,1).equals("&")){
                    lianjie += val.toString().substring(1);
                }
            }

            pr = 0.8f*pr + 0.2f*0.25f;//加入跳转因子，进行平滑处理           
            String result = pr+lianjie;
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "172.16.10.15:9001");
        String pathIn1 = "hdfs://172.16.10.15:9000/user/hadoop/pagerank_fourthinput";
        String pathOut="hdfs://172.16.10.15:9000/user/hadoop/pagerank_fourthoutput";

        Job job = new Job(conf,"page rank");
        job.setJarByClass(hello.class);
        job.setMapperClass(lxnmapper.class);
        job.setReducerClass(lxnreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(pathIn1));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}