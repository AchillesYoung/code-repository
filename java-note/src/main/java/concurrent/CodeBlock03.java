package concurrent;

class Code{
	public Code(){
		System.out.println("Code的构造方法1111");
	}
	{
		System.out.println("Code的构造块2222");
	}
	static {
		System.out.println("Code的静态代码块3333");
	}
}
public class CodeBlock03{   //CodeBlock03.class--->static block---
	{
		System.out.println("CodeBlock03的构造块444");
	}
//	static {
//		System.out.println("CodeBlock03的静态代码块555");
//	}
	public CodeBlock03(){
		System.out.println("CodeBlock03的构造方法666");
	}
	public static void main(String[] args){
		System.out.println("======我是美丽分割线=========CodeBlock03的main方法777");
		new Code();
		new CodeBlock03();
	}
}
//5732146
