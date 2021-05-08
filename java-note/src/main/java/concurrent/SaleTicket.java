package concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



class Ticket//资源类
{
	private int number = 30;//模拟初始化共计30张票
	//Lock implementations provide more extensive locking operations 
	//than can be obtained using synchronized methods and statements. 
	
	private Lock lock = new ReentrantLock();//List list = new ArrayList();
	
	public void sale()
	{
		lock.lock();
		try 
		{
			if(number > 0)
			{
				System.out.println(Thread.currentThread().getName()+"\t卖出第："+(number--)+"\t还剩下："+number);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}
}

/**
 * 
 * @Description: 卖票程序复习线程知识 ,	三个售票员			卖出			30张票
 * @author zzyy
 * @date 2018年5月25日
 * 多线程编程模板:
 * 1	编程思路---上
 * 		1.1	线程			操作			资源类
 * 		1.2	高内聚	低耦合
 */
public class SaleTicket
{
	public static void main(String[] args)//main所有程序的入口
	{
		Ticket ticket = new Ticket();
		
		new Thread(() -> {for (int i = 1; i <=40; i++) ticket.sale();}, "A").start();
		new Thread(() -> {for (int i = 1; i <=40; i++) ticket.sale();}, "B").start();
		new Thread(() -> {for (int i = 1; i <=40; i++) ticket.sale();}, "C").start();
				
		

		
		
		//Thread(Runnable target, String name) 	Allocates a new Thread object.
		/*new Thread(new Runnable() {
			@Override
			public void run()
			{
				for (int i = 1; i <=40; i++) 
				{
					ticket.sale();
				}
			}
		}, "A").start();
		new Thread(new Runnable() {
			@Override
			public void run()
			{
				for (int i = 1; i <=40; i++) 
				{
					ticket.sale();
				}
			}
		}, "B").start();
		new Thread(new Runnable() {
			@Override
			public void run()
			{
				for (int i = 1; i <=40; i++) 
				{
					ticket.sale();
				}
			}
		}, "C").start();	*/	
		
		
		
	}
}


//	1 class MyThread implements Runnable
//		2 匿名内部类	












/**
* 笔记：
* 	1 class MyThread implements Runnable
	2 匿名内部类
	3 lambda Express
*/




