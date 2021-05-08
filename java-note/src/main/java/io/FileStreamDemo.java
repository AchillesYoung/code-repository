package io;


import org.junit.Test;

import java.io.*;
import java.util.Arrays;

/**
 * show how File stream work, byte stream
 *  ! read()
 *  ! read(new byte[])
 *  ! write(String.getBytes())
 */
public class FileStreamDemo {

    /**
     *inputstream读文本，容易出现乱码
     */
    @Test
    public void testError() throws IOException {
        FileInputStream fis= new FileInputStream("bytes.txt");
        byte [] buf = new byte[5];
        int len;
        //helloworld张��丰
        while ((len=fis.read(buf))!=-1) {
            System.out.print(new String(buf, 0, len));
        }
        fis.close();
    }

    /**
     * 读二进制文件
     * @throws IOException
     */
    @Test
    public void test02() throws IOException {
        FileInputStream fis= new FileInputStream("copy.jpg");
        FileOutputStream fos= new FileOutputStream("copy2.jpg");
        byte [] buf = new byte[5];
        int len;

        while ((len=fis.read(buf))!=-1) {

            fos.write(buf, 0, len);
        }
        fis.close();

    }


    @Test
    public void test01() throws IOException {
        FileInputStream fis= new FileInputStream("bytes.txt");
        System.out.println(fis.read()); // int = 97
        byte [] word = new byte[3];
        word[0]=(byte)fis.read();
        word[1]=(byte)fis.read();
        word[2]=(byte)fis.read();
        System.out.println(Arrays.toString(word));
        System.out.println(new String(word));
        fis.close();
    }

}

