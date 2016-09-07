import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;


public class GraphCsvMaker {

	static HashMap<String, Integer> nodes = new HashMap<String,Integer>();
	static HashMap<String, Double> edges = new HashMap<String,Double>();
	static HashMap<String, Integer> tags = new HashMap<String,Integer>();
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		//args[] 0:Tag File 1:Edge File 2:node Output 3:edge Output
		String line;
		Scanner sc0 = new Scanner(new File(args[0]),"UTF-8"); 
		int nodecount =0;
		while(sc0.hasNextLine()){
			line = sc0.nextLine();
			StringTokenizer st0=new StringTokenizer(line);
			String word = st0.nextToken();
			int tag = Integer.parseInt(st0.nextToken());
			tags.put(word, tag);
			nodes.put(word, nodecount);
			nodecount++;
		}
		sc0.close();
		Scanner sc1 = new Scanner(new File(args[1]),"UTF-8");
		while(sc1.hasNext()){
			line = sc1.nextLine();
			StringTokenizer st0 = new StringTokenizer(line);
			String word = st0.nextToken();
			StringTokenizer st1 = new StringTokenizer(st0.nextToken(), ",");
			String tline;
			int wordNo = nodes.get(word);
			while(st1.hasMoreTokens()){
				tline = st1.nextToken();
				StringTokenizer st2 = new StringTokenizer(tline, ":");
				String tar = st2.nextToken();
				double eValue = Double.parseDouble(st2.nextToken());
				int tarNo = nodes.get(tar);
				if(wordNo > tarNo){
					int t = wordNo;
					wordNo=tarNo;
					tarNo = t;
				}
				String edge = new String(wordNo+","+tarNo);
				edges.put(edge, eValue);
			}
		}
		sc1.close();
		PrintWriter pr2 = new PrintWriter(new File(args[2]),"UTF-8");
		Set<Entry<String,Integer>> set = tags.entrySet();
		Iterator<Entry<String,Integer>> it0= set.iterator();
		//write file
		pr2.println("id,label,tag");
		while(it0.hasNext()){
			Entry<String,Integer> entry = it0.next();
			String word = entry.getKey();
			int tag = entry.getValue();
			int nodeId = nodes.get(word);
			pr2.println(nodeId+","+word+","+tag);
		}
		pr2.close();
		PrintWriter pr3 = new PrintWriter(new File(args[3]));
		Set<Entry<String, Double>> set0 = edges.entrySet();
		Iterator<Entry<String, Double>> it1 = set0.iterator();
		int edgeCount =0;
		//write file
		pr3.println("Source,Target,Type,id,weight");
		while(it1.hasNext()){
			Entry<String, Double> entry=it1.next();
			String edge = entry.getKey();
			double weight = entry.getValue();
			pr3.println(edge+",Undirected,"+edgeCount+","+weight);
			++edgeCount;
		}
		pr3.close();
		System.out.println("Processing sucess!");
	}
}
