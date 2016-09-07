package TagClassfier;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TagClassfier {

	private static int max_iteration = 25;
	HashMap<String,Integer> older = new HashMap<String,Integer>();
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		boolean continueTag = true;
		int truns = 0;
		while(continueTag && truns < max_iteration){
			Configuration conf=new Configuration();
			conf.set("fs.hdfs.impl.disable.cache", "true");
			HashMap<String, Integer> older = new HashMap<String,Integer>();
			FileSystem hdfs=FileSystem.get(conf);
			Scanner sc = new Scanner(hdfs.open(new Path("./RawTag.txt")),"UTF-8");
			while(sc.hasNextLine()){
				StringTokenizer st0 = new StringTokenizer(sc.nextLine());
				String word = st0.nextToken();
				String tag = st0.nextToken();
				older.put(word, Integer.parseInt(tag));
			}
			sc.close();
			try{
				Job job=new Job(conf,"TagClassfier");
				job.setJarByClass(TagClassfier.class);
				job.setMapperClass(TagClassfierMapper.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job, new Path("./taskTransferOutput"));
				FileOutputFormat.setOutputPath(job, new Path("./taskTmp"));
				job.waitForCompletion(true);
			}catch(Exception e){
				e.printStackTrace();
			}
			hdfs.delete(new Path("./taskTmp"), true);
			HashMap<String, Integer> newer = new HashMap<String,Integer>();
			Scanner sc2 = new Scanner(hdfs.open(new Path("./RawTag.txt")),"UTF-8");
			while(sc2.hasNextLine()){
				StringTokenizer st0 = new StringTokenizer(sc2.nextLine());
				String word = st0.nextToken();
				String tag = st0.nextToken();
				newer.put(word, Integer.parseInt(tag));
			}
			sc2.close();
			Set<Entry<String, Integer>> set = older.entrySet();
			Iterator<Entry<String, Integer>> it=set.iterator();
			int noChangeCount =0;
			while(it.hasNext()){
				Entry<String, Integer> entry = it.next();
				String word = entry.getKey();
				int v1 = entry.getValue();
				int v2 = newer.get(word);
				if(v1 ==v2) noChangeCount++;
			}
			if(noChangeCount > 0.9*older.size()) continueTag=false;
			++truns;
		}
	}
}
