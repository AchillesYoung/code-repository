package concurrent;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @author yangshu
 * @version Id: ThreadLocalDemo.java, v 0.1 2020/12/13 12:01 AM yangshu Exp $$
 */
public class ThreadLocalDemo implements Runnable {

    private static final ThreadLocal<SimpleDateFormat> formatter
            = ThreadLocal.withInitial(() -> {
        return new SimpleDateFormat("yyyMMdd HHmm");
    });


    @Override
    public void run() {

        System.out.println("Thread Name=" + Thread.currentThread().getName() +
                "default formatter=" + formatter.get().toPattern());
        try {
            Thread.sleep(new Random().nextInt(1000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //formatter pattern is changed here by thread, but it won't reflect to other threads
        formatter.set(new SimpleDateFormat());
        System.out.println("Thread Name="+Thread.currentThread().getName()+
                "formatter ="+formatter.get().toPattern());

    }
    public static void main(String[] args) throws InterruptedException {
        ThreadLocalDemo obj = new ThreadLocalDemo();
        for(int i=0 ; i<10; i++){
            Thread t = new Thread(obj, ""+i);
            Thread.sleep(new Random().nextInt(1000));
            t.start();
        } }
}
