package Lambda;

import java.util.Comparator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

public class AnonymousInnerClass{
    public static void main(String[] args) {
        Runnable runnable = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("inner class");
            }
        });
        runnable.run();
        Thread sss = new Thread(() -> {
            System.out.println("Sss");
        });
        sss.start();
        sss.run();//这里只是一个普通方法没有开启线程
        //多次启动一个线程是非法的。特别是当线程已经结束执行后，不能再重新启动。
//        sss.start();
        System.out.println(sss);

        //有返回值，这样写
        TreeMap<String, String> ts = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Long.compare(o1.length(), o2.length());
            }
        });
        //若 Lambda 体中只有一条语句， return 和 大括号都可以省略不写
        TreeSet<String> ts2 = new TreeSet<>((o1,o2) -> Long.compare(o1.length(),o2.length()));
        TreeSet<String> ts3 = new TreeSet<>(Comparator.comparingLong(String::length));

        Consumer consumer =  (x)-> {
            System.out.println(x);
        };
            consumer.accept("consumer");
    }
}
