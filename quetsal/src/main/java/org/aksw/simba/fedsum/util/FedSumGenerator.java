package org.aksw.simba.fedsum.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 *  Generate FedSummaries for a set of federation members (SPARQL endpoints)
 *  @author saleem
  */
public class FedSumGenerator {
public static  BufferedWriter bw ;
/**
 * initialize input information for data summaries generation
 * @param location Directory location of the resulting FedSummaries file (i.e. location/FedSum.n3)
 * @throws IOException 
 */
public FedSumGenerator(String location) throws IOException 
{
	bw= new BufferedWriter(new FileWriter(new File(location))); //--name/location where the summaries file will be stored
	bw.append("@prefix ds:<http://aksw.org/fedsum/>.");
	bw.newLine();
}
/**
 * Build FedSum data summaries for the given list of SPARQL endpoints
 * @param endpoints List of SPARQL endpoints url
 * @throws IOException 
 * @throws QueryEvaluationException 
 * @throws MalformedQueryException 
 * @throws RepositoryException 
 */
public void generateSummaries(List<String> endpoints) throws IOException, RepositoryException, MalformedQueryException, QueryEvaluationException{
 for(String endpoint:endpoints)
	{
		System.out.println("generating summaries for: " + endpoint);
	    ArrayList<String> lstPred = getPredicates(endpoint);
	 //System.out.println(lstPred);
	bw.append("#---------------------"+endpoint+" Summaries-------------------------------");
	bw.newLine();
	bw.append("[] a ds:Service ;");
	bw.newLine();
	bw.append("     ds:url   <"+endpoint+"> ;");
	bw.newLine();
	for(int i =0 ;i<lstPred.size();i++)
	{
		 System.out.println((i+1)+" in progress: " + lstPred.get(i));
		 bw.append("     ds:capability");
		 bw.newLine();
		 bw.append("         [");
		 bw.newLine();
		 bw.append("           ds:predicate  <"+lstPred.get(i)+"> ;");
		 writeSbjAuthority(lstPred.get(i),endpoint);
		 writeObjAuthority(lstPred.get(i),endpoint);
		 bw.newLine();
		 bw.append("         ] ;");
		 bw.newLine();
	 }
    bw.append("             .");
    bw.newLine();

	}
	//     bw.append("     sd:totalTriples \""+totalTrpl+"\" ;");
			 bw.append("#---------End---------");
       	 bw.close();
}
/**
 * Write all the distinct subject authorities having predicate p. 
 * consider <http://www.dbpedia.com/schema/xyz> rdf:type <http://www.dbpedia.com/schema/actor> 
 *  we are interested in http://www.dbpedia.com part of subject 
 * @param predicate  Predicate
 * @param endpoint Endpoint URL
 * 
 * @throws MalformedQueryException 
 * @throws RepositoryException 
 * @throws QueryEvaluationException 
 * @throws IOException 
 */
public void writeSbjAuthority(String predicate, String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException, IOException {
	ArrayList<String> sbjAuthorities = new ArrayList<String>();
	String strQuery = getSbjAuthorityQury(predicate);
	SPARQLRepository repo = new SPARQLRepository(endpoint);
	TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
	TupleQueryResult res = query.evaluate();
	 while (res.hasNext()) 
	 {
		 String[] sbjPrts = res.next().getValue("s").toString().split("/");
		 String sbjAuth =sbjPrts[0]+"//"+sbjPrts[2];
		if(!sbjAuthorities.contains(sbjAuth))
			sbjAuthorities.add(sbjAuth);
	  }
	 repo.getConnection().close();
	 if(!sbjAuthorities.isEmpty())
	 {
	   bw.newLine();
	   bw.append("           ds:sbjAuthority ");

	   for(int authority=0; authority<sbjAuthorities.size();authority++)
	  {
		  String strAuth = sbjAuthorities.get(authority);
		   if(authority==sbjAuthorities.size()-1)
			  bw.write("<"+strAuth + "> ; ");
		 else
			 bw.write("<"+ strAuth+ ">, ");
	  }
	 }
	}
///**
// * Get a SPARQL query to retrieve all the subject authorities for a predicate
// * Note: Due to a limit of 10000 results per query on a SPARQL endpoint, we are using Regular expressions in queries
// * to get the required part in each qualifying triples rather than doing a local SPLIT operation on results
// * @param predicate predicate
// * @return query Required SPARQL query
// */
//public String getSbjAuthorityQury(String predicate) {
//	
//	String query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
//			+ "WHERE \n"
//			+ "{ \n "
//			+ "   ?s <"+predicate+"> ?o. \n"
//			+ "  BIND(STRBEFORE(str(?s),REPLACE(str(?s), \"^([^/]*/){3}\", \"\")) AS ?authPath) \n"
//					+ "   Filter(isURI(?s)) \n"
//					+ "}" ;
//		return query;
//}
/**
 *  Get a SPARQL query to retrieve all distinct subjects for retrieving all distinct subject authorities for a predicate
 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greather than that limit
 * @param predicate Predicate
 * @return query Required SPARQL query
 */
public String getSbjAuthorityQury(String predicate) {
	String query = "SELECT DISTINCT ?s  \n"
				+ "WHERE \n"
				+ "{  \n"
				+ "   ?s <"+predicate+"> ?o. "
				+ "   Filter(isURI(?s)) \n"   //only URI can have authority
				+ "}" ;
	
		return query;
}
/**
 * Write all the distinct object authorities having predicate p. 
 * consider <http://www.dbpedia.com/schema/xyz> rdf:type <http://www.dbpedia.com/schema/actor> 
 *  we are interested in http://www.dbpedia.com part of object. Note: for rdf:type we store all the classes 
 * @param predicate  Predicate
 * @param endpoint Endpoint URL
 * 
 * @throws MalformedQueryException 
 * @throws RepositoryException 
 * @throws QueryEvaluationException 
 * @throws IOException 
 */
public void writeObjAuthority(String predicate, String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException, IOException {
	ArrayList<String> objAuthorities = new ArrayList<String>();
	String strQuery = getObjAuthorityQury(predicate);
	SPARQLRepository repo = new SPARQLRepository(endpoint);
	TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
	TupleQueryResult res = query.evaluate();
	 while (res.hasNext()) 
	 {
		if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
			objAuthorities.add(res.next().getValue("o").toString());
		else
		{
		 String[] objPrts = res.next().getValue("o").toString().split("/");
		 if ((objPrts.length>1))
		 {
		   String objAuth =objPrts[0]+"//"+objPrts[2];
		   if(!objAuthorities.contains(objAuth))
		   objAuthorities.add(objAuth);  
		}
		 }
	  }
	 repo.getConnection().close();
	 if(!objAuthorities.isEmpty())
	 {
	   bw.newLine();
	   bw.append("           ds:objAuthority ");

	   for(int authority=0; authority<objAuthorities.size();authority++)
	  {
		  String strAuth = objAuthorities.get(authority);
		   if(authority==objAuthorities.size()-1)
			  bw.write("<"+strAuth + "> ; ");
		 else
			 bw.write("<"+ strAuth+ ">, ");
	  }
	 }
	}
///**
// * Get a SPARQL query to retrieve all the object authorities for a predicate
// * Note: Due to a limit of 10000 results per query on a SPARQL endpoint, we are using Regular expressions in queries
// * to get the required part in each qualifying triples rather than doing a local SPLIT operation on results. For
// * rdf:type we retrieve the complete set of objects as usually the number of distinct classes in a dataset is usually not too many.
// * @param predicate Predicate
// * @return query Required SPARQL query
// */
//public String getObjAuthorityQury(String predicate) {
//	String query;
//	if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
//	{
//		 query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
//				+ "WHERE \n"
//				+ "{  \n"
//				+ "   ?s <"+predicate+"> ?authPath. \n"
//			
//						+ "}" ;
//	}
//	
//	else
//	{
//	 query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
//			+ "WHERE \n"
//			+ "{  \n"
//			+ "   ?s <"+predicate+"> ?o. \n"
//			+ "  BIND(STRBEFORE(str(?o),REPLACE(str(?o), \"^([^/]*/){3}\", \"\")) AS ?authPath) \n "
//					+ "   Filter(isURI(?o)) \n"
//					+ "}" ;
//	}
//		return query;
//}
/**
 *  Get a SPARQL query to retrieve all distinct objects for retrieving all distinct object authorities for a predicate
 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greather than that limit
 * @param predicate Predicate
 * @return query Required SPARQL query
 */
public String getObjAuthorityQury(String predicate) {
	String query = "SELECT DISTINCT ?o  \n"
				+ "WHERE \n"
				+ "{  \n"
				+ "   ?s <"+predicate+"> ?o. "
				+ "   Filter(isURI(?o)) \n"   //only URI can have authority
				+ "}" ;
	
		return query;
}
/**
 * Get Predicate List
 * @param endPointUrl SPARQL endPoint Url
 * @return  predLst Predicates List
 * @throws MalformedQueryException 
 * @throws RepositoryException 
 * @throws QueryEvaluationException 
 */
private static ArrayList<String> getPredicates(String endPointUrl) throws RepositoryException, MalformedQueryException, QueryEvaluationException 
{
	ArrayList<String>  predLst = new ArrayList<String>();
	String strQuery = getPredQury();
	SPARQLRepository repo = new SPARQLRepository(endPointUrl);
	TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
	TupleQueryResult res = query.evaluate();
	 while (res.hasNext()) 
	 {
		predLst.add(res.next().getValue("p").toString());	  		
	  }
	 repo.getConnection().close();
	return predLst;
}
//--------------------------------------------------------------------------
/**
 * Get SPARQL query to retrieve all predicates in a SAPRQL endpoint
 * @return query SPARQL query
 */
	private static String getPredQury() {
					String query = "SELECT DISTINCT ?p  "
						+ " WHERE "
						+ "{"
						+ "	?s ?p ?o"
						+ "} " ;
				return query;
	}
}