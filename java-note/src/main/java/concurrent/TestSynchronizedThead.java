package concurrent;

import java.util.HashMap;

/**
 * @author yangshu
 * @version Id: TestSynchronizedThead.java, v 0.1 2020/12/12 5:18 PM yangshu Exp $$
 */
public class TestSynchronizedThead {

    public static void main(String[] args) {
//
//        MyThreads myThreads = new MyThreads();
//        Thread thread = new Thread(myThreads, "thread1");
//        Thread thread2 = new Thread(myThreads, "thread2");
//        thread.start();
//        thread2.start();
//        myThreads.printCount();
        HashMap<String, String> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put(null,"@2");
        objectObjectHashMap.put(null,"3");
        objectObjectHashMap.put("key","val");
        System.out.println(objectObjectHashMap);
        System.out.println(objectObjectHashMap.get(null));
        HashMap<Integer,String> map = new HashMap<>();
//
//        Integer key = new Integer(1);
//
//        String value = "hashmap";
//
//        map.put(key,value);
//
//        System.out.println(map); //{1=hashmap}

//        key = null;

        System.gc();

        System.out.println(objectObjectHashMap); //{1=hashmap}
    }


}

class MyThreads implements Runnable {
    public static int count = 0;

    @Override
    public void run() {

        for (int i = 0; i < 5; i++) {
            countAdd();
//                if (Thread.currentThread().getName() == "thread1") {
//                    countAdd();
//                }else{
//                    printCount();
//                }
//
        }


    }

    //synchronized修饰的方法，锁的是对象，
    public synchronized void countAdd() {
        try {
            System.out.println("线程名:" + Thread.currentThread().getName() + "count add:" + count++);
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void printCount() {

        System.out.println("线程名:" + Thread.currentThread().getName() + "_print:" + count);
    }
}


