package Lambda;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.File;
import java.util.Comparator;
import java.util.List;

/**
 * lambda的本质还是作为函数式接口的实例，只有一个抽象方法
 */
public class LambdaDemo {
     
    public void LambdaGrammar() {
        /**
         * 箭头->左边是参数列表，右边是方法体
         *    格式一：没有参数，没有返回值的lambda
         */

        Runnable run = new Runnable() {
            @Override
            public void run() {
                System.out.println("lambda");
            }
        };
        System.out.println("*****lambda*****");
        Runnable run1 = () -> System.out.println("lambda");

        Runnable run2 = () -> {
            System.out.println("lambda2");
            System.out.println("lambda2");
        };
        run1.run();
        run2.run();
        /**
         *    格式二：需要有参数，没有返回值的lambda
         */
    }


     
    public void TestLambda01() {
        Comparator<Integer> comparator = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                System.out.println("compare");
                return Integer.compare(o1, o2);
            }
        };
        System.out.println(comparator.compare(12, 13));
        System.out.println("*****lambda*****");
        System.out.println("*****lambda*****");
        Comparator<Integer> comparator2 = (Integer o1, Integer o2) -> {
            System.out.println("compare+lambda");
            return Integer.compare(o1, o2);
        };

        System.out.println(comparator2.compare(15, 13));
        /**
         *    格式：有参数（不需要指明参数类型，可又jvm推断得出，有返回值的lambda
         */
        System.out.println("*****方法的引用让lambda更简洁*****");
        Comparator<Integer> comparator3 = Integer::compare;
        System.out.println(comparator3.compare(15, 25));
    }
}
