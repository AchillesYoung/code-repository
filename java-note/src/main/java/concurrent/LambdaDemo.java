package concurrent;

@FunctionalInterface
interface Foo
{
	public int add(int x, int y);
	
	
	default int div(int x, int y)
	{
		return x/y;
	}
	
	public static int sub(int x, int y)
	{
		return x - y;
	}

}


/**
 * 
 * @Description: Lambda Express-----> 函数式编程
 * @author zzyy
 * @date 2018年5月25日
 * Lambda Express
 * 1	前提，接口里面有且仅有一个方法声明
 * 2	拷贝中括号----写死右箭头----落地大括号
 * 3	@FunctionalInterface标注
 * 4	default 	方法实现
 * 5	静态方法实现
 * 
 */
public class LambdaDemo
{
	public static void main(String[] args)
	{
//		Foo foo = new Foo() {
//			@Override
//			public void sayHello()
//			{
//				System.out.println("******hello bigdata 0308");
//			}
//
//			@Override
//			public void say886()
//			{
//				System.out.println("******say886 bigdata 0308");
//			}
//		};
//		foo.sayHello();
//		foo.say886();
		
		//Lambda Express-
		
		Foo foo = null;
		foo = (int x,int y) -> {
			System.out.println("******add method");
			return x + y;
		};
		
		System.out.println(foo.add(3, 1));
		System.out.println(foo.div(10, 5));
		System.out.println(Foo.sub(10, 3));
	}
}







