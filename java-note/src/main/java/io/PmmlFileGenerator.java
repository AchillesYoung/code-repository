package io;

import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * TODO
 *
 * @author yangshu
 * @version 5.0.0
 * @created at 2020/9/25-8:05 PM
 * copyright @2020 Beijing Morong Information Techology CO.,Ltd.
 */
public class PmmlFileGenerator {

    @Test
    public void testFileReader() throws IOException {
        //提供点端，实例化文件对象
        File file = new File("/Users/shuyang/Documents/re_learn/Advanced/pmml/gamma.pmml");

//        System.out.println(file.isFile());
        //相对于当前module下的目录/Users/shuyang/Documents/Learnspace/StreamAndSerialize/path.txt
        //提供具体流
        char [] buf = new char[3888];

        FileReader fr = new FileReader(file);
        FileWriter fw = new FileWriter("/Users/shuyang/Documents/re_learn/Advanced/pmml/gammaOut",true);


        //call read

        String sb = new String();
        int n=0;
        int len;
        while ((len=fr.read(buf))!=-1){
            String tmp =new String(buf,0,len);
            n++;
            System.out.println("the num of char readed:"+len+"_"+n);
            fw.write(sb + ("clobValue" + String.valueOf(n) + ":='" + tmp + "';"));
        }
        fr.close();
    }
}
