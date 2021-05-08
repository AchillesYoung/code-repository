package Lambda;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 方法引用（ :: ）
 * 对象 :: 实例方法
 * 类 ::静态方法/实例方法
 */
//当要传递给Lambda体的操作，已经有实现的方法了，可以使用方法引用！
public class MethodCallDemo {
    /**
     * 当我们想要实现一个函数式接口的那个抽象方法，
     * 但是已经有类实现了我们想要的功能，
     * 这个时候我们就可以用方法引用来直接使用现有类的功能去实现
     */

    public static void main(String[] args) {
        Person p1 = new Person("Zeking", 18, 60);
        Person p2 = new Person("David", 38, 1);
        Person p3 = new Person("Lucky", 28, 99);

        List<Person> personList = new ArrayList<>();
        personList.add(p1);
        personList.add(p2);
        personList.add(p3);
        //引文内部类只有一个抽象方法，满足函数接口编程，所以，可以直接用lambd 吗
        TestInterface testInterface = () -> System.out.println("test");
        testInterface.test();
        //1. 因为我们的sort方法的第二个参数是一个接口，所以我们需要实现一个匿名内部类
        Collections.sort(personList, new Comparator<Person>() {
            @Override
            public int compare(Person o1, Person o2) {
                return o1.getAge().compareTo(o2.getAge());
            }
        });
        //2. 因为第二个参数是一个@FunctionalInterface的函数式接口，所以我们可以用lambda写法
        // 但是刚好又有代码(Comparator.comparing)已经实现了这个功能
        // 这个时候我们就可以采用方法引用了
        Comparator<Person> comparing = Comparator.comparing(Person::getAge);
        Person person = new Person();
        Collections.sort(personList, Comparator.comparing(Person::getAge));

        //3. 直接调用任意对象的实例方法，如 obj::equals 代表调用 obj 的 equals 方法与接口方法参数比较是否相等，效果等同 obj.equals(t);。
        //当前类的方法可用this::method进行调用，父类方法同理。
        Supplier<Integer> s = p1::getAge;
        Predicate<Person> p = p1::equals;

        System.out.println(p.test(p1));  //  false
        //数组
        Function<Integer,Person[]> f  = x->new Person[x];
        Function<Integer,Person[]>  f2 = Person[]::new;

        System.out.println(f.apply(3).length);
        System.out.println(f2.apply(2).length);
    }
}


