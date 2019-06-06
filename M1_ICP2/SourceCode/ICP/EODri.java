package com.code.eo;


import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.FileInputFormat; 
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner; 

public class EODri extends Configured implements Tool { 

	@Override
	public int run(String[] args) throws Exception 
	{ 
		if (args.length < 2) 
		{ 
			System.out.println("Please enter valid arguments"); 
			return -1; 
		} 

		JobConf conf = new JobConf(EODri.class); 
		FileInputFormat.setInputPaths(conf, new Path(args[0])); 
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
		conf.setMapperClass(EOMap.class); 
		conf.setReducerClass(EORed.class); 
		conf.setMapOutputKeyClass(Text.class); 
		conf.setMapOutputValueClass(IntWritable.class); 
		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(IntWritable.class); 

		JobClient.runJob(conf); 
		return 0; 
	} 


	public static void main(String args[]) throws Exception 
	{ 
		int exitcode = ToolRunner.run(new EODri(), args); 
		System.out.println(exitcode); 
	} 
} 

