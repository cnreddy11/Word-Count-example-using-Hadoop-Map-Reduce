/*
 
  Author : Nikhila Chireddy
  Date : 02 - 06 - 2017

*/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class NGramMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lines = value.toString().split("\n");
		Ngram ngrams[]=new Ngram[lines.length];
		int i=0;
		for(String s: lines){
			Ngram newngram = new Ngram(s);
			ngrams[i] = newngram;
			i++;
		}
		Configuration conf =context.getConfiguration();
		int Profile = conf.getInt("Profile",0);
		
		switch(Profile){
		case 1: for(i=0;i<ngrams.length;i++)
				{
					//ngrams[i].unigramGenerator();
					System.out.println(Profile);
					for(String j : ngrams[i].unigram)
						context.write(new Text(j), new Text(ngrams[i].year));
				}
				break;
		case 2: for(i=0;i<ngrams.length;i++)
				{
					//ngrams[i].unigramGenerator();
					for(String j : ngrams[i].unigram)
						context.write(new Text(j), new Text(ngrams[i].author));
				}
				break;
		case 3: for(i=0;i<ngrams.length;i++)
				{
					//ngrams[i].bigramGenerator();
					for(String j : ngrams[i].bigram)
						context.write(new Text(j), new Text(ngrams[i].year));
				}
				break;
		case 4: for(i=0;i<ngrams.length;i++)
				{
					//ngrams[i].bigramGenerator();
					for(String j : ngrams[i].bigram)
						context.write(new Text(j), new Text(ngrams[i].author));
				}
				break;
		
		}
	}
}