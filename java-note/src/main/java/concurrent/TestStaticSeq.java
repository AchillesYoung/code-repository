package concurrent;


class Father   ///Father.class--Son.class -Father Instance--Son Instance
{
	public Father(){
		System.out.println("111111");
	}
	{
		System.out.println("222222");
	}
	static{
		System.out.println("333333");
	}
}
class Son extends Father
{
	public Son()
	{
		System.out.println("444444");
	}
	{
		System.out.println("555555");
	}
	static{
		System.out.println("666666");
	}	
}
public class TestStaticSeq
{
	public static void main(String[] args)
	{
		new Son();//从父到子，静态先行
		System.out.println("======================");
		new Son();
		System.out.println("======================");
		new Father();
	}
}
