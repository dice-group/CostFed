package org.aksw.simba.start;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.aksw.simba.quetsal.util.TBSSSummariesGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Operation;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.structures.QueryInfo;



public class FeasibleQueryEvalaution {
	protected static final Logger log = LoggerFactory.getLogger(FeasibleQueryEvalaution.class);
	/**
	 * write results into file
	 */
	public static BufferedWriter bw ;
	/**
	 * @param args
	 * @throws Exception 
	 */
	
public static void main(String[] args) throws Exception 
	{
//	File qryFile = new File(args[0]);
//	String[] queries = getQueries(qryFile);
	// curQuery = args[0];
//	System.out.println("Total queries to execute: "+ (queries.length-1) );
//	for(int i=1 ; i < queries.length ; i++ )		
//		System.out.println(":-------------------------------------\n"+queries[i]);
//	BasicConfigurator.configure();
	//BasicConfigurator.configure();
	// TODO Auto-generated method stub
	//Config.initialize();
//	Config.getConfig().set("enableMonitoring", "true");
//	Config.getConfig().set("monitoring.logQueryPlan", "true");
	List<String> endpoints = loadEndpoints();
	String cfgName = "costfed.props";
	Config config = new Config(cfgName);
	SailRepository repo = FedXFactory.initializeSparqlFederation(config, endpoints);
	SailRepositoryConnection con = repo.getConnection();
	//	System.out.println("Repo is initialized with all endpoints..."); 
//	String results = "D:/BigRDFBench/completeness_correctness/results.n3";
   // ResultsLoader.loadResults(results);
	//SailRepository repo = FedXFactory.initializeSparqlFederation(endpoints);
	
	
	
	//String tq = "Select * where { ?s ?p ?o} limit 10";
//	File Querydir = new File("queries/");
//	File[] listOfQueryFiles = Querydir.listFiles();
//  for (File qryFile : listOfQueryFiles)
//	{	
	// File qryFile = new File(args[0]);
	 bw= new BufferedWriter(new FileWriter(new File("results"+System.currentTimeMillis()+".txt")));	
	//String[] queries = getQueries(qryFile);
	// curQuery = args[0];
	File qryFile = new File(args[0]);
	String[] queries = getQueries(qryFile);
	//queries[0] = "Select * where { ?s ?p ?o} ";
	//System.out.println("Total queries to execute: "+ (queries.length-1) ); 	
	long qmTime = 0, timeout = 180000 ; 
	List<String> timeOutQueries = new ArrayList<String> ();
	for(int i=1 ; i < queries.length ; i++ )
	{
	System.out.println(i+":-------------------------------------\n"+queries[i].replace("\n", " "));
	bw.write(i+":-------------------------------------\n"+queries[i].replace("\n", " ")+"\n");
	
	Runtime.getRuntime().gc();
	TupleQueryResult res = null;
	 System.out.println("Resultset iterator is set to null...");
	 long sTime = System.currentTimeMillis();
	TupleQuery query = con.prepareTupleQuery(QueryLanguage.SPARQL, queries[i]); 
	
      long count = 0;
      query.setMaxQueryTime((int) (timeout/1000));
       try{
     res = query.evaluate();
    //
 //   if(args[0].equals("-qr")){
   
    while(res.hasNext() && (System.currentTimeMillis()-sTime)<timeout)  
	{
    	res.next();
	  //  System.out.println(count);
	  //  bw.write(count +": "+res.next()+"\n");
		count++;
	}
      } catch(Exception ex){ log.error(" error in running query. processing by-passed ..",ex); bw.write(" error in running query. processing by-passed but the query has most probably zero results due to empty statement pattern.\n"); }
  System.out.println("Total Number of Records: " + count ); 
  bw.write("Total Number of Records: " + count+"\n");
  long runTime =System.currentTimeMillis()-sTime;
 
  if (runTime > timeout)
  {
	  runTime = timeout;
	  timeOutQueries.add(queries[i]);
  }
 
  System.out.println(": Query execution time (msec):"+ runTime+"\n");
  bw.write(": Query execution time (msec):"+ runTime+"\n");
  bw.flush();
  qmTime = qmTime + runTime ;
  System.out.println("Total time so far (ms) : " + qmTime);
   }
	 //  }
	   // if(args[0].equals("-qr")){
		// long runTime =System.currentTimeMillis()-startTime;
	System.out.println("\n############## Total timeout queries: " + timeOutQueries.size()+" ##############");
	bw.write("\n############## Total timeout queries: " + timeOutQueries.size()+" ##############\n");
	for(String query:timeOutQueries){
		  System.out.println(": Query timeout :"+ query.replace("\n", " ")+"\n");
		  bw.write(": Query timeout :"+ query.replace("\n", " ")+"\n");
	}
	    
	System.out.println("\n-----\nQuery Mix execution time (msec):"+ qmTime);
	  bw.write("\n-----\nQuery Mix execution time (msec):"+ qmTime);  
		bw.close();  
	//	   MonitoringImpl ms = null ;
		  // System.out.println(QueryPlanLog.getQueryPlan());
		 //  MonitoringUtil.printMonitoringInformation();
			//System.out.println(ms.getAllMonitoringInformation());
		  // System.out.println("Total Number of Records: " + count+"\n");
	//}
	// System.out.println(StatsGenerator.getFscores("D:/workspace/BigRDFBench-Utilities/results/"+queryNo, query.evaluate()));
	// System.out.println("Missing:" + StatsGenerator.getMissingResults("D:/workspace/BigRDFBench-Utilities/results/"+queryNo, res));
     //  FederationManager.getInstance().shutDown();
	 //  System.out.println("Done. Results written into results/"+args[1]);
	 //  bw.close();
   	//String logDir = "D:/BigRDFBench/endpoints/logs/";
	//System.out.println("Endpoint requests:" + RequestCount.getTotalEndpointRequests(logDir));
	//RequestCount.clearLogs(logDir);

	   System.exit(0);
	

	}
public static List<String> loadEndpoints() throws IOException {
	List<String> endpoints = new ArrayList<String>();
	BufferedReader br = new BufferedReader(new FileReader("endpoints"));
	String line;
	while ((line = br.readLine()) != null)
	{
		endpoints.add(line);
		System.out.println(line);
	}
    br.close();
		return endpoints;
	}
/**
 * Load query string from file
 * @param qryFile Query File
 * @return query Query string
 * @throws IOException
 */
public static String[]  getQueries(File qryFile) throws IOException {
	String fileStr= "" ; 
	BufferedReader br = new BufferedReader(new FileReader(qryFile));
	String line;
	while ((line = br.readLine()) != null)
	{
		fileStr = fileStr+line+"\n";
	}
    br.close();
    
	return fileStr.split("#-------------------------------------------------------");
}


}
