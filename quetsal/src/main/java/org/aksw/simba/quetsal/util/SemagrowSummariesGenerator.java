package org.aksw.simba.quetsal.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLRepository;

public class SemagrowSummariesGenerator {
	static Logger log = Logger.getLogger(SemagrowSummariesGenerator.class);
	
	public BufferedWriter bw ;
	public double distinctSbj;
	public long trplCount ;
	
	/**
	 * initialize input information for data summaries generation
	 * @param location Directory location of the resulting FedSummaries file (i.e. location/FedSum.n3)
	 * @throws IOException IO Exceptions
	 */
	public SemagrowSummariesGenerator(String location) throws IOException 
	{
		bw = new BufferedWriter(new FileWriter(new File(location))); //--name/location where the summaries file will be stored
		bw.append("@prefix void: <http://rdfs.org/ns/void#> ."); bw.newLine();
		bw.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . "); bw.newLine();
		bw.append("@prefix dc: <http://purl.org/dc/elements/1.1/> ."); bw.newLine();
		bw.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ."); bw.newLine();
		bw.newLine();
		bw.append("_:DatasetRoot rdf:type void:Dataset .");
		bw.newLine();
	}
	
	public static void main(String[] args) throws IOException
	{
		//String host = "ws24348.avicomp.com";
		String host = "192.168.0.145";
		List<String> endpoints = Arrays.asList(
			 //"http://" + host + ":8890/sparql",
			 //"http://" + host + ":8891/sparql",
			 //"http://" + host + ":8892/sparql"
			 //, 
				"http://" + host + ":8893/sparql"
			 /*
			 , "http://" + host + ":8894/sparql",
			 "http://" + host + ":8895/sparql",
			 "http://" + host + ":8896/sparql",
			 "http://" + host + ":8897/sparql",
			 "http://" + host + ":8898/sparql"
			 
			 , "http://" + host + ":8887/sparql"
			 , "http://" + host + ":8888/sparql"
			 , "http://" + host + ":8889/sparql"
			 , "http://" + host + ":8899/sparql"
			 */
		);


		String outputFile = "summaries/semagrow_8893.ttl";
	//	String namedGraph = "http://aksw.org/fedbench/";  //can be null. in that case all graph will be considered 

		SemagrowSummariesGenerator generator = new SemagrowSummariesGenerator(outputFile);
		long startTime = System.currentTimeMillis();

		generator.generateSummaries(endpoints);
		//generator.generateDAWSummaries(endpoints, namedGraph, branchLimit, 0.5);
		log.info("Data Summaries Generation Time (min): "+ (double)(System.currentTimeMillis() - startTime) / (1000 * 60));
		log.info("Data Summaries are secessfully stored at "+ outputFile);
	}
	
	ExecutorService executorService;
	/**
	 * Build Quetzal data summaries for the given list of SPARQL endpoints
	 * @param endpoints List of SPARQL endpoints url
	 * @param graph Named graph. Can be null. In this case all named graphs will be considered for Quetzal summaries
	 * @param branchLimit Branching limit
	 * @throws IOException IO Error
	 */
	public void generateSummaries(List<String> endpoints) throws IOException
	{
		executorService = Executors.newFixedThreadPool(10);
		
		List<Future<?>> flist = new ArrayList<Future<?>>();
		AtomicInteger dsnum = new AtomicInteger(0);
		for (String endpoint : endpoints)
		{
			Future<?> future = executorService.submit(new Runnable() {
			    public void run() {
			    	String sum = generateSummary(endpoint, dsnum.incrementAndGet());
			    	synchronized(bw) {
			    		try {
			    			bw.append(sum);
			    			bw.flush();
			    		} catch (Exception e) {
			    			log.error(e);
			    		}
			    	}
			    }
			});
			flist.add(future);
		}

		for (Future<?> f : flist) {
			try {
				f.get();
			} catch (Exception e) {
    			log.error(e);
			}
		}
		executorService.shutdown();
		bw.close();
	}

	public String generateSummary(String endpoint, int dsnum)
	{
		StringBuilder sb = new StringBuilder();
		
		long totalTrpl = 0;
		List<String> lstPred = getPredicates(endpoint, null);
		log.info("total distinct predicates: "+ lstPred.size() + " for endpoint: " + endpoint);
			
		sb.append("_:Dataset").append(dsnum).append("\n");
		sb.append("  rdf:type void:Dataset ;").append("\n");
		sb.append("  void:sparqlEndpoint <" + endpoint + "> ;").append("\n");
		sb.append("  void:properties " + lstPred.size() + " ;").append("\n");
			
		for (int i = 0; i < lstPred.size(); i++)
		{
			log.info((i+1)+" in progress: " + lstPred.get(i) + ", endpoint: " + endpoint);
			sb.append("  void:propertyPartition [ ").append("\n");
			sb.append("    void:property  <" + lstPred.get(i) + "> ;").append("\n");
			
			long tripleCount = getTripleCount(lstPred.get(i), endpoint);
			sb.append("    void:triples " + tripleCount + " ;").append("\n");
			
			long distinctSbj = getDistinctSbj(lstPred.get(i), endpoint);
			sb.append("    void:distinctSubjects " + distinctSbj + " ;").append("\n");
			
			long distinctObj = getDistinctObj(lstPred.get(i), endpoint);
			sb.append("    void:distinctObjects " + distinctObj + " ] ;").append("\n");
			
			totalTrpl += tripleCount;
		}
		sb.append("  void:triples " + totalTrpl + " ;").append("\n");
			
		long totalSbj = getSubjectCount(endpoint);
		sb.append("  void:distinctSubjects " + totalSbj + " ;").append("\n");
		
		long totalObj = getObjectCount(endpoint);
		sb.append("  void:distinctObjects " + totalObj + " ;").append("\n");
		sb.append("  void:subset _:DatasetRoot .").append("\n").append("\n");

		return sb.toString();
	}
	
	/**
	 * Get total number of distinct objects for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 */
	public static long getDistinctObj(String pred, String endpoint) {
		String strQuery = "SELECT  (COUNT(DISTINCT ?o) AS ?objs) " + // 
				"WHERE " +
				"{" +
	       		"?s <" + pred + "> ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult rs = query.evaluate();
			return Long.parseLong(rs.next().getValue("objs").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	
	/**
	 * Get total number of distinct objects for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 */
	public static long getDistinctSbj(String pred, String endpoint) {
		String strQuery = "SELECT  (COUNT(DISTINCT ?s) AS ?subjs) " + // 
				"WHERE " +
				"{" +
	       		"?s <" + pred + "> ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult rs = query.evaluate();
			return Long.parseLong(rs.next().getValue("subjs").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	
	/**
	 * Get total number of distinct subjects of a dataset
	 * @return count 
	 */
	public static long getSubjectCount(String endpoint) {
		String strQuery = "SELECT  (COUNT(DISTINCT ?s) AS ?sbjts) " + // 
				"WHERE " +
				"{" +
	       		"?s ?p ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult rs = query.evaluate();
			return Long.parseLong(rs.next().getValue("sbjts").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	/**
	 * Get total number of distinct objects of a dataset
	 * @return count
	 */
	public static long getObjectCount(String endpoint) {
		String strQuery = "SELECT  (COUNT(DISTINCT ?o) AS ?objts) " + // 
				"WHERE " +
				"{" +
	       		"?s ?p ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult rs = query.evaluate();
			return Long.parseLong(rs.next().getValue("objts").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	/**
	 * Get total number of triple for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 */
	public static Long getTripleCount(String pred, String endpoint) {
		String strQuery = "SELECT  (COUNT(?s) AS ?triples) " + // 
				"WHERE " +
				"{" +
	       		"?s <"+pred+"> ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult rs = query.evaluate();
			return Long.parseLong(rs.next().getValue("triples").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	
	///**
	// * Get a SPARQL query to retrieve all the subject authorities for a predicate
	// * Note: Due to a limit of 10000 results per query on a SPARQL endpoint, we are using Regular expressions in queries
	// * to get the required part in each qualifying triples rather than doing a local SPLIT operation on results
	// * @param predicate predicate
	// * @return query Required SPARQL query
	// */
	//public String getSbjAuthorityQuery(String predicate) {
	//	
	//	String query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
	//				+ "WHERE \n"
	//				+ "{ \n "
	//				+ "   ?s <"+predicate+"> ?o. \n"
	//				+ "  BIND(STRBEFORE(str(?s),REPLACE(str(?s), \"^([^/]*/){3}\", \"\")) AS ?authPath) \n"
	//						+ "   Filter(isURI(?s)) \n"
	//						+ "}" ;
	//		return query;
	//}

	/**
	 *  Get a SPARQL query to retrieve all distinct subjects for retrieving all distinct subject authorities for a predicate
	 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greater than that limit
	 * @param predicate Predicate
	 * @param graph Named graph
	 * @return query Required SPARQL query
	 */
	public String getSbjAuthorityQuery(String predicate, String graph) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT DISTINCT ?s");
		if (null != graph) {
			sb.append(" FROM <");
			sb.append(graph);
			sb.append(">");
		}
		sb.append(" WHERE { ?s <");
		sb.append(predicate);
		sb.append("> ?o. FILTER (isURI(?s)) }");
		return sb.toString();
	}
	
	/**
	 * Get Predicate List
	 * @param endPointUrl SPARQL endPoint Url
	 * @param graph Named graph
	 * @return  predLst Predicates List
	 */
	private static List<String> getPredicates(String endPointUrl, String graph)
	{
		List<String>  predLst = new ArrayList<String>();
		String strQuery = getPredQuery(graph);
		SPARQLRepository repo = new SPARQLRepository(endPointUrl);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult res = query.evaluate();
			while (res.hasNext()) 
			{
				String pred = res.next().getValue("p").toString();
				predLst.add(pred);	  		
			}
		} finally {
			conn.close();
			repo.shutDown();
		}
		return predLst;
	}

	//--------------------------------------------------------------------------
	/**
	 * Get SPARQL query to retrieve all predicates in a SAPRQL endpoint
	 * @param graph Named Graph
	 * @return query SPARQL query
	 */
	private static String getPredQuery(String graph) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT DISTINCT ?p");
		if (null != graph) {
			sb.append(" FROM <");
			sb.append(graph);
			sb.append(">");
		}
		sb.append(" WHERE { ?s ?p ?o }");
		return sb.toString();
	}
}