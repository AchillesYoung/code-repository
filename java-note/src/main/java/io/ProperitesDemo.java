package io;

import org.junit.Test;

import java.io.*;
import java.util.Properties;

public class ProperitesDemo {
    /**
     *   setProperty(String key,String value);
     *   getProperty(String key):根据键获取值
     *   load(InputStream|Reader )加载配置文件的键值对到Properties对象
     *   store(OutputStream|Writer)：将Properties中的键值对存储到指定输出流中
     *   list(PrintStream):将Properties中的键值对列出到指定设备

     */

    @Test
    public void test1() throws FileNotFoundException, IOException {

        //1.创建Properties对象
        Properties pro = new Properties();

        //2.读取db.properites（文件——>内存）
        pro.load(new FileInputStream("db.properties"));

        //3.遍历Properties集合
        //操作1：将所有的键值对打印到指定控制台
		pro.list(System.out);
        //操作2：根据键获取值
        String name = pro.getProperty("name");
        String pwd = pro.getProperty("password");
        System.out.println(name+"\t"+pwd);
    }
    @Test
    public void test2() throws FileNotFoundException, IOException {
        //1.创建Properties对象
        Properties pro = new Properties();

        //2.为Properties保存键值对信息(内存)
        pro.setProperty("ip", "192.168.10.165");
        pro.setProperty("port", "8888");

        //3.将pro中的键值对存储在配置文件（内存——》文件）
        FileWriter writer = new FileWriter("test.properties");
        pro.store(writer,"test");

    }


}
