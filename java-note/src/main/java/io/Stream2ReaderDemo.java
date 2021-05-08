package io;

import org.junit.Test;

import java.io.*;
import java.nio.Buffer;

/**
 *InputStreamReader:Reader的子类，实现InputStream——>Reader 编码
 *OutputStreamWriter:Writer的子类，实现Writer->OutputStream 解码
 * 字符集
 */
public class Stream2ReaderDemo {
    ////测试InputStreamReader  :将InputStream——>Reader
    @Test
    public void test1() throws IOException {
        BufferedReader reader =
                new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream("test.txt"),"UTF-8"));
        char [] buf =new char[20];
        int len;
        while ((len=reader.read(buf))!=-1){
            System.out.print(new String(buf,0,len));

        }
        reader.close();

    }

    @Test
    public void test2() throws IOException {
        BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream("test.txt"),"UTF-8"));

        BufferedWriter writer =
                new BufferedWriter(
                    new OutputStreamWriter(
                        new FileOutputStream("convert.txt")));
        //什么区别呢
//        BufferedWriter writer = new BufferedWriter(new FileWriter("bytes.txt"));
//   1.当处理纯文本数据时，如果使用字符流效率更高，并且可以有效解决中文问题，所以建议将字节流转换成字符流
//   2.可以在使用时指定编码格式
        char [] buf =new char[20];
        int len;
        while ((len=reader.read(buf))!=-1){
            writer.write(buf,0,len);
        }
        writer.close();
    }

}
