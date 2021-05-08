package Lambda;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 左边（类或者对象）:: 方法名
 * 1 对象:: 非静态
 * 2 类:: 静态
 * 3 类:: 非静态
 */
public class MethodRefDemo {


    public void test01() {

        Consumer<String> con = str -> System.out.println(str);
        con.accept("Consumer");
        //方法引用
        PrintStream ps = System.out;
        Consumer<String> con2 = System.out :: print;
        /**
         * 要求抽象方法的行参列表和返回值类型与方法引用的行参列表和返回值类型相同
         */
        con2.accept("method reference consumer");
    }

    public void test02() {
        Comparator<Integer> con = (x,y) -> Integer.compare(x,y);
        System.out.println(con.compare(12,15));
        /**
         * 要求抽象方法的行参列表和返回值类型与方法引用的行参列表和返回值类型相同
         */
        //方法引用
        Comparator<Integer> con2 = Integer::compare;

        System.out.println(con2.compare(123,15));
    }

    /**
     * 数组引用
     */
    public void test03() {

        Function<Integer,String[]> fun = len -> new String[len];
        String[] arr = fun.apply(5);

        System.out.println(Arrays.toString(arr));
        //数组引用  类同于构造器引用

        Function<Integer,String[]> fun2 =  String[] :: new;
        arr = fun2.apply(6);
        System.out.println(Arrays.toString(arr));

    }

}
