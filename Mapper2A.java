/*
 
  Author : Nikhila Chireddy
  Date : 02 - 06 - 2017

*/

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Mapper2A extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		Ngram ngrams[]=new Ngram[lines.length];
		int i=0;
		for(String s: lines){
			Ngram newngram = new Ngram(s);
			ngrams[i] = newngram;
			i++;
		}
		for(i=0;i<ngrams.length;i++)
		{
			//ngrams[i].unigramGenerator();
			
			for(String j : ngrams[i].bigram)
				context.write(new Text(j), new Text(ngrams[i].year));
		}
		
		
	}
}