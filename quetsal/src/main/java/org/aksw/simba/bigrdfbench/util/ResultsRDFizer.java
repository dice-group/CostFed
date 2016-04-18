package org.aksw.simba.bigrdfbench.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class ResultsRDFizer{

	public static BufferedWriter bw;
public static void generateRDFResults(String queriesLocation, String resultsLocation, String outputLocation) throws IOException
 {
	 bw = new BufferedWriter(new FileWriter(new File(outputLocation+"results.nt")));
	  bw.write("@prefix bigrdfbench:<http://bigrdfbench.aksw.org/schema/> . ");
	  File queriesDir = new File(queriesLocation);
	  File[] queryFiles = queriesDir.listFiles();
	  if (queryFiles != null) 
	  {
	    for (File queryFile : queryFiles)
	     	writeQueryResults(queryFile,resultsLocation,outputLocation);
	     System.out.println("RDFized results are stored at "+outputLocation+"results.nt");
	    }
	  else
	      System.out.println("Directory Error");
	  bw.close();
 }
	private static void writeQueryResults(File queryFile, String resultsLocation, String outputLocation) throws IOException
	{
		
		String queryString = getQueryString(queryFile.getCanonicalPath().toString());
    	String queryName = queryFile.getName().replace(".txt", "");
    	bw.newLine();
    	bw.write("<http://aksw.org/bigrdfbench/resource/"+queryName+"> bigrdfbench:queryName <http://aksw.org/bigrdfbench/query/"+queryName+"> .");
    	bw.newLine();
    	bw.write("<http://aksw.org/bigrdfbench/query/"+queryName+"> bigrdfbench:queryString \""+queryString +"\" .");
    	//System.out.println(queryString);
    	FileInputStream fstream = new FileInputStream(resultsLocation+queryName+".txt");
  	    DataInputStream in = new DataInputStream(fstream);
  	    @SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
  	    String[] bindingNames = br.readLine().split("\t");
  	    //String[] bindingValues = new String[bindingNames.length];
  	       String line="";
  	       System.out.println(queryFile.getName()+": consersion in pgrogress...");
  	       String bVal ="";
  	       int count = 1;
  	   	  while (( line= br.readLine()) != null)
  	   	  {
  	   		String [] results = line.split("\t");
  	   		bw.newLine();
  	        bw.write("<http://aksw.org/bigrdfbench/query/"+queryName+"> bigrdfbench:result <http://aksw.org/bigrdfbench/result/r"+count+"> .");    
  	        for(int i =0 ; i<results.length;i++)
   		  	       {
  	        	    bw.newLine();
  	                bw.write("<http://aksw.org/bigrdfbench/result/r"+count+"> bigrdfbench:bindingName <http://aksw.org/bigrdfbench/binding/"+bindingNames[i]+"> .");
  	   	            if(results[i].startsWith("http://") || results[i].startsWith("ftp://"))
	   				bVal =  "<" + results[i] +">";
  	   	            else
  	                 bVal =   results[i] ;

  	   	            bw.newLine();
  	   	            bw.write("<http://aksw.org/bigrdfbench/binding/"+bindingNames[i]+"> bigrdfbench:bindingValue "+bVal+" .");
	           
   		  	       }
  	        count++;
  	   	 
  	   	  }	   
}
	public static String getQueryString(String queryFile) throws IOException
	{
	    String SPARQLqry = "";
	 	FileInputStream fstream = new FileInputStream(queryFile);
	  // Get the object of DataInputStream
	    DataInputStream in = new DataInputStream(fstream);
	    @SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String line="";
	   	  while (( line= br.readLine()) != null)
	   	  {
	   		 SPARQLqry = SPARQLqry+line; 
	   	  }
	return SPARQLqry;
    }
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
        generateRDFResults("D:/BigRDFBench/completeness_correctness/queries/","D:/BigRDFBench/completeness_correctness/results/", "D:/BigRDFBench/completeness_correctness/");
	}

}
