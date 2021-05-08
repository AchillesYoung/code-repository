package Lambda;

import org.junit.Test;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author yangshu06 <yangshu06@kuaishou.com>
 * Created on 2021-03-11
 */
public class CommonStreamUtils {
    @Test
    public void mapStream() {
        List<String> one = new ArrayList<>();
        Collections.addAll(one, "赵信", "武则天", "虞姬", "项羽", "干将莫邪");
        Stream<String> stream = one.stream();
        List<String> collect = stream.filter((x) -> (x == "赵信")).collect(Collectors.toList());
        Function<String, Object> getBytes = String::getBytes;
        Map<String, Object> collect2 = one.stream().collect(Collectors.toMap(String::toString, getBytes));

        System.out.println(collect);
        System.out.println(collect2);
    }

    @Test
    public void optionalDemo() {
        List<Integer> someNumbers = Arrays.asList(1, 2, 3, 4, 5);
        Optional<Integer> res = someNumbers.stream().map(x -> x * x).filter(x -> x % 3 == 0).findFirst();
        System.out.println(res.isPresent());
        System.out.println(res);
        System.out.println(res.get());


    }

}
