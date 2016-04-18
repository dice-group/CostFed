package org.aksw.simba.quetsal.core;

import java.util.ArrayList;
import java.util.List;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;

public class SourceRanking {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static List<StatementSource> getRankedTriplePatternSources(StatementPattern stmt, List<StatementSource> stmtSrces, int k) throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		List<StatementSource> stmtRankedSrces = new ArrayList<StatementSource>();
		if (boundPredicate(stmt) && !boundSubject(stmt) && !boundObject(stmt) )   //?s <p> ?o
		{	String p= stmt.getPredicateVar().getValue().toString(); 
			String  queryString = getPredLookupQuery(p,stmtSrces) ;
			stmtRankedSrces = getRankedSources(queryString,k);
			}
		else if (boundPredicate(stmt) && !boundSubject(stmt) && boundObject(stmt) )  //?s <p> <o>
		{	String p= stmt.getPredicateVar().getValue().toString(); 
		 	String  queryString = getPred_ObjLookupQuery(p,stmtSrces) ;
		// 	System.out.println(queryString);
		 	stmtRankedSrces = getRankedSources(queryString, k);
		}
		else if (boundPredicate(stmt) && boundSubject(stmt) && !boundObject(stmt) ) //<s> <p> ?o
		{	String p= stmt.getPredicateVar().getValue().toString(); 
		 	String  queryString = getPred_SbjLookupQuery(p,stmtSrces) ;
		 	stmtRankedSrces = getRankedSources(queryString, k);
		}
		else if (!boundPredicate(stmt) && !boundSubject(stmt) && boundObject(stmt) ) //?s ?p <o>
		{	String p= stmt.getPredicateVar().getValue().toString(); 
		 	String  queryString = getObjLookupQuery(p,stmtSrces) ;
		 	stmtRankedSrces = getRankedSources(queryString, k);
		}
		else if (!boundPredicate(stmt) && boundSubject(stmt) && !boundObject(stmt) ) //<s> ?p ?o
		{	
		 	String  queryString = getSbjLookupQuery(stmtSrces) ;
		 	stmtRankedSrces = getRankedSources(queryString, k);
		}
		else if (!boundPredicate(stmt) && boundSubject(stmt) && boundObject(stmt) ) //<s> ?p <o>
		{	
		 	String  queryString = getSbj_ObjLookupQuery(stmtSrces) ;
		 	stmtRankedSrces = getRankedSources(queryString, k);
		}
		else if (!boundPredicate(stmt) && !boundSubject(stmt) && !boundObject(stmt) ) //?s ?p ?o
		{	
		 	String  queryString = getPred_Sbj_ObjLookupQuery(stmtSrces) ;
		 	stmtRankedSrces = getRankedSources(queryString, k);
		}
      //  System.out.println("cardinality: " + card);
		return stmtRankedSrces;
}
	public static String getPred_Sbj_ObjLookupQuery(List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  ?url ?card "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?card . "
				+ "        \n?s ds:url ?url ."        
				+ "        \n} ORDER BY DESC(?card)";
		return queryString;
	}
	
	
	public static String getSbj_ObjLookupQuery(List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT ?url ((?triples * 1/?ssel * 1/osel) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
				+ "		   \n?s ds:totalSbj ?ssel ."
				+ "		   \n?s ds:totalObj ?osel ."
				+ "        \n?s ds:url ?url ."        
				+ "        \n} ORDER BY DESC(?card)";
		return queryString;
	}
	
	public static String getSbjLookupQuery(List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  ?url ((?triples * 1/?sel) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
				+ "		   \n?s ds:totalSbj ?sel ."
				+ "        \n?s ds:url ?url ."        
				+ "        \n} ORDER BY DESC(?card)";
		return queryString;
	}
	
	public static String getObjLookupQuery(String p,List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  ?url ((?triples * 1/?sel) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
				+ "		   \n?s ds:totalObj ?sel ."
				+ "        \n?s ds:url ?url ."        
				+ "        \n} ORDER BY DESC(?card)";
		return queryString;
	}

		public static String getPred_SbjLookupQuery(String p,List<StatementSource> stmtSrces) {
			String union = getEndpointUnion(stmtSrces);
			String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
					+ "SELECT  ?url ((?triples * ?sel) AS ?card) "
					+ " WHERE { \n" + union 
					+ " 	   \n?s ds:capability ?cap . "
					+ "		   \n?cap ds:predicate <" + p + "> ."
					+ "        \n?cap ds:triples ?triples . "
					+ "        \n?cap ds:avgSbjSelectivity ?sel ."
					+ "        \n?s ds:url ?url ."        
					+ "        \n} ORDER BY DESC(?card)";
			return queryString;
		}
	

	public static List<StatementSource> getRankedSources(String queryString, int k) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		List<StatementSource> rankedSources = new ArrayList<StatementSource>();
		TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		//System.out.println(queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		int count = 0 ;
		while(result.hasNext() && count <k)
		{
			String endpoint = result.next().getValue("url").stringValue();
			String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
			StatementSource src =  new StatementSource(id, StatementSourceType.REMOTE);
			rankedSources.add(src);
			count++;
		}

		return rankedSources;
	}
	public static String getPred_ObjLookupQuery(String p,List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  ?url ((?triples * ?sel) AS ?card) "
				+ " WHERE { \n" + union 
				+ " 	   \n?s ds:capability ?cap . "
				+ "		   \n?cap ds:predicate <" + p + "> ."
				+ "        \n?cap ds:triples ?triples . "
				+ "        \n?cap ds:avgObjSelectivity ?sel ."
				+ "        \n?s ds:url ?url ."        
				+ "        \n} ORDER BY DESC(?card)";
		return queryString;
	}
	public static String getPredLookupQuery(String p,List<StatementSource> stmtSrces) {
	
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  ?url ((?triples) AS ?card) "
				+ " WHERE { \n" + union 
				+ " 	   \n?s ds:capability ?cap . "
				+ "		   \n?cap ds:predicate <" + p + "> ."
				+ "        \n?cap ds:triples ?triples ."
				+ "        \n?s ds:url ?url ."        
				+ "        \n} ORDER BY DESC(?card)";
		return queryString;
	}

	private static String getEndpointUnion(List<StatementSource> stmtSrces) {
		String union = "";
		for(StatementSource s:stmtSrces)
		{
			if(union.equals(""))
			union= "{ ?s ds:url  <"+ s.getEndpointID().replace("sparql_", "http://").replace("_", "/") +"> . }" ;
			else
			union= union+ "\n UNION \n{  ?s ds:url  <"+ s.getEndpointID().replace("sparql_", "http://").replace("_", "/") +"> . }" ;
		}
		return union;
	}

	public static boolean boundPredicate(StatementPattern stmt) {
		if(stmt.getPredicateVar().getValue()!=null)
			return true;
		else
		return false;
	}
	
	public static boolean boundSubject(StatementPattern stmt) {
		if(stmt.getSubjectVar().getValue()!=null)
			return true;
		else
		return false;
	}
	
	public static boolean boundObject(StatementPattern stmt) {
		if(stmt.getObjectVar().getValue()!=null)
			return true;
		else
		return false;
	}

}
