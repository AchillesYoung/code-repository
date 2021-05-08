package io;


import org.junit.Test;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 网络联系为什么是IO stream
 * question： chinese encode and reader read how many bytes everytime
 */
public class FileReaderDemo {

    public static void main(String[] args) {
        File file = new File("path.txt");
        System.out.println(file.getAbsoluteFile());
        //相对于当前工程下的目录Users/shuyang/Documents/Learnspace/path.txt
    }
    @Test
    public void testFileReader() throws IOException {
        //提供点端，实例化文件对象
        File file = new File("path.txt");
        //相对于当前module下的目录/Users/shuyang/Documents/Learnspace/StreamAndSerialize/path.txt
        System.out.println(file.getAbsoluteFile());
        //提供具体流
        FileReader fr = new FileReader(file);
        //call read
        System.out.println(fr.getEncoding());
        int data;
        while ((data=fr.read())!=-1){
            System.out.print((char)data);
        }
        fr.close();
    }


    @Test
    public void testFileWriter() throws IOException {
        String str = "中";
        System.out.println(str.getBytes().length);

        // true -->append mode
        FileReader fr= new FileReader("path.txt");
        FileWriter fw = new FileWriter("outfile",true);

        char [] buf = new char[100];
        int len;
        while ((len = fr.read(buf))!=-1){
            fw.write(buf,0,len);
        }
        fw.close();
        fr.close();
        //①flush ②close
    }

    @Test
    public void test03() throws IOException {
        File file = new File("path.txt");
        FileReader fr = new FileReader(file);
        char [] buf = new char[100];
        int len;

        while ((len=fr.read(buf))!=-1){
            System.out.println("\nthe num of char readed:"+len);

            System.out.print("string readed:"+new String(buf,0,len));

        }

        fr.close();
    }

    @Test
    public void test02() throws IOException {
        File file = new File("path.txt");
        FileReader fr = new FileReader(file);
        char [] buf = new char[100];
        int len;
        while ((len=fr.read(buf))==100){
            System.out.print(new String(buf));
        }
        System.out.print("string readed:"+new String(buf,0,len));
        fr.close();
    }
}
