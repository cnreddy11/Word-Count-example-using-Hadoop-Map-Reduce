/*
 
  Author : Nikhila Chireddy
  Date : 02 - 06 - 2017

*/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MainClass {
public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {
	
	if (args.length != 3) {
		System.out.printf("Usage: <jar file> <input dir> <output dir> <Profile>\n");
		System.exit(-1);
	}
	int option=0;
	String profile = args[2];
	switch(profile){
	case "1A" : option = 1;
				break;
	case "1B" : option = 2;
				break;
	case "2A" : option = 3;
				break;
	case "2B" : option = 4;
	            break;
	default : System.out.println("Enter a valid Profile : 1A, 1B, 2A or 2B");
	          System.exit(-1);
	}
	Configuration conf =new Configuration();
	
	Job job=Job.getInstance(conf);
	job.setJarByClass(MainClass.class);
	switch(option){
	case 1:	job.setMapperClass(Mapper1A.class);
			break;
	case 2:	job.setMapperClass(Mapper1B.class);
			break;
	case 3:	job.setMapperClass(Mapper2A.class);
			break;
	case 4:	job.setMapperClass(Mapper2B.class);
			break;
			
	}
	job.setReducerClass(NGramReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}