package mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.Task;

import java.io.IOException;

public class userStatistic {
	
	
	public static class MyMap extends Mapper<LongWritable,Text,Text, userWritable>{
		
		private int overheader=-1;
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			overheader++;
			if(overheader!=0){
				String[] review=value.toString().split(",");
				String[] userID=review[0].split(":");
				int[] votes=new int[3];
				int rating=Integer.valueOf(review[3]);
				for(int i=0;i<3;i++){
					votes[i]=Integer.valueOf(review[4+i]);
				}
				userWritable user=new userWritable(votes[0],votes[1],votes[2],rating);
				context.write(new Text(userID[0]), user);
				Counter ct=(Counter) context.getCounter("Job Counters", "SLOTS_MILLIS_MAPS");
				ct.increment(1);
			}
			
			
			
		}
	}
	
	public static class Reduce extends Reducer<Text, userWritable, Text, userWritable> {
		public void reduce(Text key, Iterable<userWritable> values, Context context) throws IOException, InterruptedException {
			int contU=0;
			int contF=0;
			int contC=0;
			int contR=0;
			int cont=0;
			for(userWritable u : values){
				cont++;
				contU+=u.getU();
				contF+=u.getF();
				contC+=u.getC();
				contR+=u.getR();
			}
			userWritable user=new userWritable(contU,contF,contC,contR/cont);
			context.write(key, user);
			Counter ct=(Counter) context.getCounter("Job Counters", "SLOTS_MILLIS_REDUCES");
			ct.increment(1);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
	    int count=Integer.valueOf(args[1]);
	    for(int i=0;i<count;i++){
	    	Runtime.getRuntime().gc();
	    	long initm=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
	    	long startTime=System.currentTimeMillis();
	    	Configuration conf = new Configuration();
		    Job job = new Job(conf, "userStatistic");
		    job.setOutputKeyClass(Text.class);//Set the "Key" class for the job output data:theClass - the key class for the job output data.
		    job.setOutputValueClass(userWritable.class);//Set the "Value" class for job outputs.
		           
		    job.setMapperClass(MyMap.class);//Set the Mapper for the job.
		    job.setReducerClass(Reduce.class);//Set the Reducer for the job.
		            
		    job.setInputFormatClass(TextInputFormat.class);//Set the InputFormat for the job.
		    job.setOutputFormatClass(TextOutputFormat.class);//Set the OutputFormat for the job.
		            
		    FileInputFormat.addInputPath(job, new Path(args[2+i]));
		    String path=String.join("_", args[0], String.valueOf(i));
		    FileOutputFormat.setOutputPath(job, new Path(path));
		    job.waitForCompletion(true);
		    Runtime.getRuntime().gc();
		    long endm=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
		    long endTime=System.currentTimeMillis();
		    System.out.printf("Memory %d \n Time: %d \n",initm-endm,endTime-startTime);
	    }	    

	}

}