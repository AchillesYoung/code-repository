package io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * TODO
 *
 * @author yangshu
 * @version 5.0.0
 * @created at 2019/12/11-5:30 PM
 * copyright @2019 Beijing Morong Information Techology CO.,Ltd.
 */
public class MyInputDemo {
    public static void main(String[] args) {
//		System.out.println("请输入姓名：");
//		String name = MyInput.readString();
//		System.out.println(name);
//		System.out.println("请输入年龄：");
//		int age = MyInput.readInt();
//		System.out.println(name+"\t"+age);
//		MyInput.close();

        Scanner input = new Scanner(System.in);
//        input.close();
        input.next();
        input.nextInt();
    }

}
class MyInput{

    static BufferedReader reader = null;


    public static int readInt() {

        int num = Integer.parseInt(readString());
        return num;
    }

    /**
     * 功能：接受键盘输入的字符串，并返回
     * @return
     */
    public static String readString(){


        String line = null;
        try {

            reader = new BufferedReader(new InputStreamReader(System.in));
            line = reader.readLine();

            return line;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 关闭
     */
    public static void close(){
        if(reader!=null){
            try {
                reader.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


}
