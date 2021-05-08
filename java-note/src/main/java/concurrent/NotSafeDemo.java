package concurrent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 * 
 * @Description: 集合类不安全
 * @author zzyy
 * @date 2018年5月25日
 * 笔记见后：
 */
public class NotSafeDemo
{
	public static void main(String[] args)
	{
		Map<String,String> map = new ConcurrentHashMap<String,String>();
		for (int i = 1; i <=30; i++) 
		{
			new Thread(() -> {
				map.put(Thread.currentThread().getName(), UUID.randomUUID().toString().substring(0, 6));
				System.out.println(map);
			},String.valueOf(i)).start();
		}		
	}

	public static void setNotSafe()
	{
		Set<String> set = new CopyOnWriteArraySet<String>();
		
		for (int i = 1; i <=30; i++) 
		{
			new Thread(() -> {
				set.add(UUID.randomUUID().toString().substring(0, 6));
				System.out.println(set);
			},String.valueOf(i)).start();
		}
	}

	public static void listNotSafe()
	{
		List<String> list = new CopyOnWriteArrayList<String>();//new ArrayList<String>();
//		
//		list = Arrays.asList("a","b","c");
//		
//		list.forEach(System.out::println);
		
		/**
		CopyOnWrite容器即写时复制的容器。往一个容器添加元素的时候，不直接往当前容器Object[]添加，而是先将当前容器Object[]进行Copy，
		复制出一个新的容器Object[] newElements，然后新的容器Object[] newElements里添加元素，添加完元素之后，
		再将原容器的引用指向新的容器 setArray(newElements);。这样做的好处是可以对CopyOnWrite容器进行并发的读，
		而不需要加锁，因为当前容器不会添加任何元素。所以CopyOnWrite容器也是一种读写分离的思想，读和写不同的容器
		public boolean add(E e) {
		    final ReentrantLock lock = this.lock;
		    lock.lock();
		    try {
		        Object[] elements = getArray();
		        int len = elements.length;
		        Object[] newElements = Arrays.copyOf(elements, len + 1);
		        newElements[len] = e;
		        setArray(newElements);
		        return true;
		    } finally {
		        lock.unlock();
		    }
		}			Collection	Collections
		写时复制
		Collection 接口
		Collections 工具类				Collections.synchronizedList(list)
		*/
		
		
		for (int i = 1; i <=30; i++) 
		{
			new Thread(() -> {
				list.add(UUID.randomUUID().toString().substring(0, 6));
				System.out.println(list);//[02309x,iladsf9,oiajds9]
			},String.valueOf(i)).start();
		}
		//java.util.ConcurrentModificationException
	}
}





















































