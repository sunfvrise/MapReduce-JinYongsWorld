package PageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GraphBuilder{

	@SuppressWarnings("deprecation")
	public static void main(String[] args){
		try{
			Configuration conf = new Configuration();
			Job job =new Job(conf,"Graph Builder");
			job.setJarByClass(GraphBuilder.class);
			job.setMapperClass(GraphBuliderMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static class GraphBuliderMapper extends Mapper<Object,Text,Text,Text>
	{
		public void map(Object key,Text value,Context context)
					throws IOException,InterruptedException{
						String pagerank ="1.0\t";
						StringTokenizer str = new StringTokenizer(value.toString());
						Text page;
						if(str.hasMoreTokens())
							 page= new Text(str.nextToken());
						else
							return;
						pagerank += str.nextToken();
						context.write(page,new Text(pagerank));
				}
	}

}
