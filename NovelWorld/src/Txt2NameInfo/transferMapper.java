package Txt2NameInfo;

import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import org.ansj.splitWord.analysis.*;

import org.ansj.domain.Term;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

@SuppressWarnings("deprecation")
public class transferMapper extends Mapper<Object,Text,Text,Text> {
	
	private org.apache.hadoop.fs.Path[] NameFilePath;

	@SuppressWarnings("resource")
	public void setup(Context context) throws IOException{
		NameFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String line;
		BufferedReader br =new BufferedReader(new java.io.FileReader(NameFilePath[0].toString()));
		while((line = br.readLine()) !=null){
			java.util.StringTokenizer sTokenizer=new java.util.StringTokenizer(line);
			org.ansj.library.UserDefineLibrary.insertWord(sTokenizer.nextToken(), "RoleName", 1000);
		}
	}
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		FileSplit FileSplit=(FileSplit) context.getInputSplit();
		String filename=FileSplit.getPath().getName();
		List<Term> After =UserDefineAnalysis.parse(value.toString());
		HashMap<String, Integer> NameMap = new HashMap<String,Integer>();
		for(Term x:After){
			if(x.getNatureStr().equals(new String("RoleName"))){
				String rName = x.getName();
				if((rName.equals(new String("大汉"))&& !filename.equals(new String("金庸01飞狐外传.txt")))) continue;
				if((rName.equals(new String("汉子"))&& !filename.equals(new String("金庸11侠客行.txt")))) continue;
				if((rName.equals(new String("说不得"))&& !filename.equals(new String("金庸12倚天屠龙记.txt")))) continue;
				NameList.add(rName);
			}
		}
		Set<Entry<String, Integer>> set = NameMap.entrySet();
		Iterator<Entry<String, Integer>> it0 = set.iterator();
		while(it0.hasNext()){
			Entry<String, Integer> aEntry = it0.next();
			Iterator<Entry<String, Integer>> it1 = set.iterator();
			while(it1.hasNext()){
				Entry<String, Integer> bEntry = it1.next();
				if(!aEntry.equals(bEntry)){
					int av = aEntry.getValue();
					int bv = bEntry.getValue();
					if(av > bv){
						int t =av;
						av=bv;
						bv =t ;
					}
					for(int i=0;i<av;i++){
						context.write(new Text(aEntry.getKey()), new Text(bEntry.getKey()));
					}
				}
			}
		}
	}
}
