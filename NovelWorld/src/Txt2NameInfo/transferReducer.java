package Txt2NameInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class transferReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		HashMap<String, Integer> vMap=new HashMap<String,Integer>();
		for(Text x:value){
			String vString=x.toString();
			if(vMap.containsKey(vString))
				vMap.put(vString, vMap.get(vString).intValue()+1);
			else vMap.put(vString, 1);
		}
		Set<Entry<String, Integer>> entry = vMap.entrySet();
		int aValue=0;
		Iterator<Entry<String, Integer>> it=entry.iterator();
		while(it.hasNext()){
			Entry<String,Integer> t=it.next();
			aValue += t.getValue();
		}
		it=entry.iterator();
		StringBuilder sb=new StringBuilder();
		
		Entry<String,Integer> t=it.next();
		String tName=t.getKey();
		double tValue=(t.getValue() * 1.0)/aValue;
		sb.append(tName+":"+tValue);
		while(it.hasNext()){
			t=it.next();
		    tName=t.getKey();
			tValue=(t.getValue() * 1.0)/aValue;
			sb.append(","+tName+":"+tValue);
		}
		context.write(key,new Text(sb.toString()));
	}
}
