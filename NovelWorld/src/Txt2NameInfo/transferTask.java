package Txt2NameInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


@SuppressWarnings("deprecation")
public class transferTask{

	public static void main(String[] args) throws Exception {
		try{
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new Path("/data/task2/people_name_list.txt").toUri(),conf);
		Job job= new Job(conf,"Transfer Task");
		job.setJarByClass(transferTask.class);
		job.setMapperClass(transferMapper.class);
		job.setReducerClass(transferReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/data/task2/novels"));
		FileOutputFormat.setOutputPath(job, new Path("./taskTransferOutput"));
		job.waitForCompletion(true);
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
	}

}
