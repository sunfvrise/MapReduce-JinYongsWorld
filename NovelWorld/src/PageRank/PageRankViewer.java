package PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankViewer {
	@SuppressWarnings("deprecation")
	public static void main(String[] args){
		try{
			Configuration conf=new Configuration();
			Job job = new Job(conf,"PageRankSort");
			job.setJarByClass(PageRankViewer.class);
			job.setMapperClass(PageRankViewerMapper.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setSortComparatorClass(DoubleWritableDecressingComparator.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public static class PageRankViewerMapper extends Mapper<Object,Text,DoubleWritable,Text>
	{
		private Text outPage = new Text();
		private DoubleWritable outPr = new DoubleWritable();
		public void map(Object key,Text value,Context context)
						throws IOException,InterruptedException
		{
			String[] line = value.toString().split("\t");
			String page = line[0];
			Double pr = Double.parseDouble(line[1]);
			outPage.set(page);
			outPr.set(pr);
			context.write(outPr,outPage);
			
		}
	}
	
	private static class DoubleWritableDecressingComparator extends DoubleWritable.Comparator
	{
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a,WritableComparable b)
		{
			return -super.compare(a,b);
		}
		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2)
		{
			return -super.compare(b1,s1,l1,b2,s2,l2);
		}
	}
}