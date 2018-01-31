/*
 
  Author : Nikhila Chireddy
  Date : 02 - 06 - 2017

*/

import java.util.ArrayList;
import java.util.List;

public class Ngram {
	public String Delimiter = "<===>" ;
	public String data;
	public String year;
	public String author;
	List<String> unigram = new ArrayList<String>();
	List<String> bigram = new ArrayList<String>();
	public Ngram(String S)
	{
		String[] line = S.toString().split(Delimiter);
		author = line[0];
		String date = line[1];
		data= line[2];
		//data = data.replaceAll("^[A-Za-z0-9]", "");
		String[] dateformat = date.toString().split(" ");
		year = dateformat[(dateformat.length)-1];	
		unigramGenerator();
		bigramGenerator();
	}
	public void unigramGenerator()
	{
		data = data.replaceAll("[^A-Z0-9a-z]", " ").trim();
		data = data.toLowerCase();
		String[] divide = data.toString().split(" ");
		for(String s: divide)
		{
			if(!s.isEmpty())
				unigram.add(s.toString());
		}
	}
	public void bigramGenerator()
	{
		if(unigram == null || unigram.size()==0)
			return;
		String temp = "_START_"+" "+unigram.get(0);
		bigram.add(temp);
		
		for(int i=1;i<unigram.size();i++)
		{
			String dummy = unigram.get(i)+" " + unigram.get(i-1);
			bigram.add(dummy.toString());
		}
		temp = unigram.get(unigram.size()-1)+" "+"_END_";
		bigram.add(temp.toString());
	}

}
