package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import sun.security.krb5.Config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

public class App  {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        String filePath = "hdfs://10.0.0.2:9000/input/test.txt";
        Path path1 = new Path(filePath);
        String putPath ="hdfs://10.0.0.2:9000/input/write.txt";
        Path path2= new Path(putPath);
        FileSystem fs = FileSystem.get(new URI(filePath), conf);
        System.out.println( "READING ============================" );
        FSDataInputStream is = fs.open(path1);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String content = br.readLine();
        System.out.println(content);
        br.close();
        System.out.println("WRITING ============================");
        byte[] buff = "this is helloworld from java api!\n".getBytes();
        FSDataOutputStream os = fs.create(path2);
        os.write(buff, 0, buff.length);
        os.close();
        fs.close();
    }
}


class TestConf{
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("dfs.replication","1");
        configuration.addResource("hdfs-site.xml");
        FileSystem fs=FileSystem.get(configuration);
        Iterator<Map.Entry<String,String>> iter=configuration.iterator();
        while (iter.hasNext()){
            Map.Entry<String,String> e=iter.next();
            System.out.println(e.getKey()+"\t"+e.getValue());
        }
    }
}

class TestHdfs{
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");
        FileSystem fs= FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> listFiles=fs.listFiles(new Path("/"),true);
        while(listFiles.hasNext()){
            LocatedFileStatus file=listFiles.next();
            System.out.println(file.getPath()+"\n");
            System.out.println(file.getPath().getName()+"\t");
            System.out.println(file.getLen()+"\t");
            System.out.println(file.getReplication()+"\t");
            BlockLocation[] blockLocations=file.getBlockLocations();
            System.out.println(blockLocations.length+"\t");
            for (BlockLocation b:blockLocations){
                String[] hosts=b.getHosts();
                System.out.println(hosts[0]+'-'+hosts[1]+'\t');
            }
            System.out.println();
        }
    }
}

class PutGet{
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://10.0.0.2:9000");
        conf.set("dfs.replication","3");
        FileSystem fs=FileSystem.get(conf);
        System.setProperty("HADOOP_USER_NAME","root");

        fs.copyFromLocalFile(new Path("/root/upload.txt"),new Path("/input/upload.txt"));
        fs.copyToLocalFile(new Path("/input/upload.txt"),new Path("/root/download.txt"));
        fs.close();
    }
}