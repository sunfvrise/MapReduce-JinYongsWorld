package PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankIter{

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args){
		try{
			Configuration conf=new Configuration();
			Job job = new Job(conf,"PageRankIter");
			job.setJarByClass(PageRankIter.class);
			job.setMapperClass(PRIterMapper.class);
			job.setReducerClass(PRIterReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
		}
		catch(Exception e){
			e.printStackTrace();;
		}
	}
	
	public static class PRIterMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context)
						throws IOException,InterruptedException
						{
							String line =value.toString();
							String[] tuple = line.split("\t");
							String pageKey=tuple[0];
							double pr = Double.parseDouble(tuple[1]);
							if(tuple.length > 2)
							{
								String [] linkPages = tuple[2].split(",");
								for(int i=0;i<linkPages.length;i++)
								{
									String[] tmp = linkPages[i].split(":");
									double ratio = Double.parseDouble(tmp[1]);
									String prValue = pageKey + "\t" + String.valueOf(pr*ratio);
									context.write(new Text(tmp[0]),new Text(prValue));
								}
								context.write(new Text(pageKey),new Text("|" + tuple[2]));
							}
						}
	}


	public static class PRIterReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		String links = "";
		double pagerank = 0;
		for( Text value:values)
		{
			String tmp = value.toString();
			if(tmp.startsWith("|")){
				links  = "\t" + tmp.substring(tmp.indexOf("|") + 1);
				continue;
			}
			String[] tuple = tmp.split("\t");
			if(tuple.length > 1)
				pagerank += Double.parseDouble(tuple[1]);
		}
		context.write(new Text(key),new Text(String.valueOf(pagerank) + links ));
		}
	}
}
