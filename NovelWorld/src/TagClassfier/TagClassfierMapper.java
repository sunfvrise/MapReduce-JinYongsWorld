package TagClassfier;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagClassfierMapper extends Mapper<Object, Text, Text, Text> {
	
	HashMap<String, Integer> older = new HashMap<String,Integer>();
	HashMap<String, Integer> newer = new HashMap<String,Integer>();
	FileSystem hdfs;
	public void setup(Context context) throws IOException{
		hdfs=FileSystem.get(context.getConfiguration());
		Scanner sc = new Scanner(hdfs.open(new Path("./RawTag.txt")),"UTF-8");
		while(sc.hasNextLine()){
			StringTokenizer st0 = new StringTokenizer(sc.nextLine());
			String word = st0.nextToken();
			String tag = st0.nextToken();
			older.put(word, Integer.parseInt(tag));
		}
		sc.close();
	}
	public void map(Object key,Text value,Context context){
		StringTokenizer st = new StringTokenizer(value.toString());
		String mainWord = st.nextToken();
		if(!older.containsKey(mainWord)) return;
		StringTokenizer st2 = new StringTokenizer(st.nextToken(),",");
		double [] list = new double [15];
		for(int i=0;i<list.length;i++) list[i]=0;
		int wordNum =0;
		while(st2.hasMoreTokens()){
			StringTokenizer st3 = new StringTokenizer(st2.nextToken(),":");
			String sname = st3.nextToken();
			double var = Double.parseDouble(st3.nextToken());
			if(!older.containsKey(sname)) continue;
			int itag = older.get(sname);
			list[itag] += var;
			wordNum++;
		}
		if(list[0] == wordNum) newer.put(mainWord, 0);
		else{
			int max =1;
			for(int i=1;i<list.length;i++){
				if(list[i] > list[max]){
					max = i;
				}
			}
			newer.put(mainWord, max);
		}
	}
	@SuppressWarnings("deprecation")
	public void cleanup(Context context) throws IOException{
		hdfs.delete(new Path("./RawTag.txt"));
		FSDataOutputStream out = hdfs.create(new Path("./RawTag.txt"));
		PrintWriter pr0 = new PrintWriter(out);
		Set<Entry<String, Integer>>set = newer.entrySet();
		Iterator<Entry<String, Integer>> iterator= set.iterator();
		while(iterator.hasNext()){
			Entry<String,Integer> entry=iterator.next();
			String word = entry.getKey();
			int tag = entry.getValue();
			pr0.println(new String(word+"\t"+tag));
		}
		pr0.close();
		out.close();
	}
}
