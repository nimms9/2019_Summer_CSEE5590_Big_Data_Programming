package com.code.chars;

import java.io.IOException;    
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.IntWritable;    
import org.apache.hadoop.io.Text;    
import org.apache.hadoop.mapred.FileInputFormat;    
import org.apache.hadoop.mapred.FileOutputFormat;    
import org.apache.hadoop.mapred.JobClient;    
import org.apache.hadoop.mapred.JobConf;    
import org.apache.hadoop.mapred.TextInputFormat;    
import org.apache.hadoop.mapred.TextOutputFormat;    
public class C_Runner {    
    public static void main(String[] args) throws IOException{    
        JobConf conf = new JobConf(C_Runner.class);    
        conf.setJobName("CharCount");    
        conf.setOutputKeyClass(Text.class);    
        conf.setOutputValueClass(IntWritable.class);            
        conf.setMapperClass(C_Mapper.class);    
        conf.setCombinerClass(C_Reducer.class);    
        conf.setReducerClass(C_Reducer.class);         
        conf.setInputFormat(TextInputFormat.class);    
        conf.setOutputFormat(TextOutputFormat.class);           
        FileInputFormat.setInputPaths(conf,new Path(args[0]));    
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));     
        JobClient.runJob(conf);    
    }    
}    