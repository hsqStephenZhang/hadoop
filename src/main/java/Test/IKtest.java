package Test;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;


public class IKtest {
    public static void main(String[] args) {
        String text = "我们在测试智能分词的运行效果实例";

        try{
            StringReader reader=new StringReader(text);
            IKSegmenter ikSegmenter=new IKSegmenter(reader,true);
            Lexeme word=null;
            String w=null;
            StringBuffer sb=new StringBuffer();
            while ((word=ikSegmenter.next())!=null){
                w=word.getLexemeText();
                if(sb.length()>0){
                    sb.append(" | ");
                }
                sb.append(w);
            }
            System.out.println(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
