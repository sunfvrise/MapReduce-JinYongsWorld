import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.StringTokenizer;

public class TagCreator {

	public static void main(String[] args) throws FileNotFoundException {
		Scanner sc0 = new Scanner(new File(args[0]),"UTF-8");
		PrintWriter pr0 = new PrintWriter(new File(args[1]));
		String line;
		while(sc0.hasNext()){
			line = sc0.nextLine();
			StringTokenizer st0=new StringTokenizer(line);
			String word = st0.nextToken();
			int tag = 0;
			if(word.equals(new String("韦小宝"))|| word.equals(new String("鳌拜"))|| word.equals(new String("阿双"))){
				tag =1;
			}
			else if(word.equals(new String("胡斐"))|| word.equals(new String("程灵素"))|| word.equals(new String("袁紫衣"))  ){
				tag =2;
			}
			else if(word.equals(new String( "胡一刀"))|| word.equals(new String("苗人凤"))|| word.equals(new String("田归农")) ){
				tag =3;			
						}
			else if(word.equals(new String("狄云"))|| word.equals(new String("丁典"))|| word.equals(new String("戚长发"))  ){
				tag =4;
			}
			else if(word.equals(new String("乔峰"))   || word.equals(new String("段誉"))  || word.equals(new String("阿朱"))){
				tag =5;
			}
			else if(word.equals(new String( "郭靖"))  || word.equals(new String("黄蓉")) || word.equals(new String("洪七公"))){
				tag =6;
			}
			else if(word.equals(new String("李文秀"))  || word.equals(new String("华辉"))|| word.equals(new String("阿曼"))){
				tag =7;
			}
			else if(word.equals(new String("令狐冲"))   || word.equals(new String("岳灵珊")) || word.equals(new String("向问天")) ){
				tag =8;
			}
			else if(word.equals(new String("陈家洛")) || word.equals(new String("香香公主"))|| word.equals(new String("霍青桐")) ){
				tag =9;
			}
			else if(word.equals(new String("杨过"))  || word.equals(new String("小龙女"))|| word.equals(new String("郭襄"))){
				tag =10;
			}
			else if(word.equals(new String("张无忌")) || word.equals(new String("杨逍"))|| word.equals(new String("张三丰"))){
				tag =11;
			}
			else if(word.equals(new String("袁承志"))|| word.equals(new String("夏青青"))|| word.equals(new String("金蛇郎君")) ){
				tag =12;
			}
			else if(word.equals(new String( "袁冠南"))|| word.equals(new String("林玉龙"))|| word.equals(new String("任飞燕")) ){
				tag =13;
			}
			else if(word.equals(new String("石破天"))   || word.equals(new String("丁不三"))|| word.equals(new String("谢烟客"))){
				tag =14;
			}
			pr0.println(word+"\t"+tag);
		}
		sc0.close();
		pr0.close();
		System.out.println("OK!");
	}

}
