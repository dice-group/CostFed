package org.aksw.simba.fedsum;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import com.fluidops.fedx.Config;
/**
 * FedSum configurations setup. Need to run one time in the start before query execution
 * @author Saleem
 *
 */
public class FedSumConfig {

	static Logger log = LoggerFactory.getLogger(Config.class);
   	public static  RepositoryConnection con = null;
   	public static  ArrayList<String> dataSources = new  ArrayList<String>() ;
   	public static ArrayList<String> commonPredicates = new ArrayList<String>(); // list of common predicates. Note we use this in ASK_dominent Source selection Algorithm
	public static double commonPredThreshold;  //A threshold value for a predicate ( in % of total data sources) to be considered in common predicate list
	public static String mode; // Index_dominant , ASK_dominant. In first type of mode we  make use of sbj, obj authorities to find relevant sources for triple patterns with bound subject or objects e.g ?s  owl:sameAs  <http://dbpedia.org/resource/Barack_Obama>, we will perform index lookup for predicate owl:sameAs and objAuthority  <http://dbpedia.org> and all the qualifying sources will be added to the set of capable sources for that triple pattern.  
	                                          // In hybrid mode we make use of SPARQL ASK queries for bound subjects or objects of a common predicate such as owl:sameAs. If Predicate is not common then we use index sbj ,obj authorities parts as explained above
	
	/**
	 * FedSum Configurations. Must call this method once before starting source selection.
	 * mode can be either set to Index_dominant or ASK_dominant. See details in FedSum paper.
	 * @param inputCommonPredThreshold Threshold value between common and normal predicates
	 * @param inputMode  Source Selection mode i.e. Index_dominant or ASK_dominant
	 * @param inputFedSummaries Summaries of all the available data sources
	 * @throws Exception 
	 */
	public static void initialize(String InputFedSummaries, String inputMode, double inputCommonPredThreshold) throws Exception 
	{
		mode = inputMode;  //{ASK_dominant, Index_dominant}
		commonPredThreshold =inputCommonPredThreshold;  //considered a predicate as common predicate if it is presenet in 33% available data sources
		//long startTime = System.currentTimeMillis();
		loadFedSummaries(InputFedSummaries);
		//System.out.println("Index Load Time: "+ (System.currentTimeMillis()-startTime));
		   
		loadDataSources();
		if(mode =="ASK_dominant")
		loadCommonPredList();
	}
/**
 * Initialize list of SPARQL endpoints from FedSummaires 
 * @throws Exception
 */
	public static void loadDataSources() throws Exception 
	{
		String queryString = "SELECT DISTINCT ?url WHERE {?s <http://aksw.org/fedsum/url> ?url }";
		TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while(result.hasNext())
		{
		   dataSources.add(result.next().getValue("url").stringValue());
		} 
	}
												
/**
 * Load common predicate list using the threshold value specified as input											
 * @throws RepositoryException
 * @throws MalformedQueryException
 * @throws QueryEvaluationException
 */
public static void loadCommonPredList() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		
	String queryString = "Prefix ds:<http://aksw.org/fedsum/> "
			+ "SELECT DISTINCT ?p "
			+ " WHERE {?s ds:predicate ?p. }";
			
    TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
	 TupleQueryResult result = tupleQuery.evaluate();
	 ArrayList<String> FedSumPredicates = new  ArrayList<String>();
	   while(result.hasNext())
	   {
		   FedSumPredicates.add(result.next().getValue("p").stringValue());
	   }
	   //---check each distinct 
	   
	   for(String predicate:FedSumPredicates)
	   {
		   int count = 0;
		   queryString = "Prefix ds:<http://aksw.org/fedsum/> "
		   		+ "SELECT  Distinct ?url "
		   	+ " WHERE {?s ds:url ?url. "
			+ " 		?s ds:capability ?cap. "
			+ "		   ?cap ds:predicate <" + predicate + "> }" ;
		   	
		     tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			 result = tupleQuery.evaluate();
			 while(result.hasNext())
			 {
				 result.next();
				 count++;
			 }
			  double threshold = (double) count/dataSources.size();
			 if(threshold>=commonPredThreshold)
				 commonPredicates.add(predicate); 
		}
	   //  System.out.println(commonPredicates);
		 }
	

/**
 * Load FedSummaries file into sesame in memory model
 * @param FedSummaries Summaries file
 */
public static void loadFedSummaries(String FedSummaries) {
	File curfile = new File ("summaries/memorystore.data");
	curfile.delete();
	File fileDir = new File("summaries\\");
	Repository myRepository = new SailRepository( new MemoryStore(fileDir) );
	try {
		myRepository.initialize();
	} catch (RepositoryException e) {
		e.printStackTrace();
	}
	    File file = new File(FedSummaries);
		
		try {
			con = myRepository.getConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		   try {
			con.add(file, "aksw.org.simba", RDFFormat.N3);
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		  
		
	}

}
