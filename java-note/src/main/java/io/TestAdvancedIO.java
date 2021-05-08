package io;

import java.io.*;

/**
 * 转换流+标准输入输出+缓冲流+重定向
 * 从键盘输入字符串，要求将读取到的整行字符串转成大写输出。
 * 然后继续进行输入操作，直至当输入“e”或者“exit”时，退出程序。
 */
public class TestAdvancedIO {
    public static void main(String[] args) throws IOException {

        //1.创建流对象(用于读取键盘输入的数据)
        BufferedReader reader  = new BufferedReader(new InputStreamReader(System.in));
        PrintStream out = System.out;//保存默认是输出设备
//		PrintStream ps = new PrintStream("src\\strings.txt");
        //2.循环读取
        String line;
        while(true){
            System.setOut(out);
            System.out.println("请输入字符串：");
            line = reader.readLine();
            if("e".equals(line)||"exit".equals(line)){
                break;
            }
            File file = new File("StreamAndSerialize/strings.txt");
            //重定向到目标设备
            System.setOut(new PrintStream(new FileOutputStream(file,true)));
            System.out.println(line.toUpperCase());
        }
        //3.关闭
        reader.close();
    }

}
