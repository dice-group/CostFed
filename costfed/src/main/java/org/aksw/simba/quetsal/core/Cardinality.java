package org.aksw.simba.quetsal.core;

import java.util.List;

import org.aksw.simba.quetsal.configuration.Summary;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.structures.QueryInfo;

public class Cardinality {
	
	public static long getTotalTripleCount(RepositoryConnection conn, List<StatementSource> stmtSrces)
	{
		String queryString = getTotalTripleCountQuery(stmtSrces) ;
		return executeQuery(conn, queryString);
	}
	
	public static long getTriplePatternCardinality(QueryInfo qi, StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		return qi.<Summary>getSummary().getTriplePatternCardinality(stmt, stmtSrces);
	}
	
	public static double getTriplePatternObjectMVKoef(QueryInfo qi, StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		return qi.<Summary>getSummary().getTriplePatternObjectMVKoef(stmt, stmtSrces);
	}
	
	public static double getTriplePatternSubjectMVKoef(QueryInfo qi, StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		return qi.<Summary>getSummary().getTriplePatternSubjectMVKoef(stmt, stmtSrces);
	}
	
	public static long getTriplePatternCardinalityOriginal(RepositoryConnection conn, StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		long card = 0;
		boolean boundP = boundPredicate(stmt), boundS = boundSubject(stmt), boundO = boundObject(stmt);
		if (boundP && !boundS && !boundO) { //?s <p> ?o
			String p = stmt.getPredicateVar().getValue().toString();
			String queryString = getPredLookupQuery(p, stmtSrces) ;
			card = executeQuery(conn, queryString);
		} else if (boundP && !boundS && boundO) { //?s <p> <o>
			String p = stmt.getPredicateVar().getValue().toString(); 
		 	String queryString = getPred_ObjLookupQuery(p, stmtSrces) ;
		 	//System.out.println(queryString);
		    card = executeQuery(conn, queryString);
		} else if (boundP && boundSubject(stmt) && !boundObject(stmt) ) //<s> <p> ?o
		{	String p= stmt.getPredicateVar().getValue().toString(); 
		 	String  queryString = getPred_SbjLookupQuery(p, stmtSrces) ;
		    card = executeQuery(conn, queryString);
		}
		else if (!boundP && !boundSubject(stmt) && boundObject(stmt) ) //?s ?p <o>
		{	String p= stmt.getPredicateVar().getValue().toString(); 
		 	String  queryString = getObjLookupQuery(p,stmtSrces) ;
		    card = executeQuery(conn, queryString);
		}
		else if (!boundP && boundSubject(stmt) && !boundObject(stmt) ) //<s> ?p ?o
		{	
		 	String  queryString = getSbjLookupQuery(stmtSrces) ;
		    card = executeQuery(conn, queryString);
		}
		else if (!boundP && boundSubject(stmt) && boundObject(stmt) ) //<s> ?p <o>
		{	
		 	String  queryString = getSbj_ObjLookupQuery(stmtSrces) ;
		    card = executeQuery(conn, queryString);
		}
		else if (!boundP && !boundS && !boundO) //?s ?p ?o
		{	
		 	String  queryString = getPred_Sbj_ObjLookupQuery(stmtSrces) ;
		    card = executeQuery(conn, queryString);
		}
      //  System.out.println("cardinality: " + card);
		return card;
	}
	
	public static String getPred_Sbj_ObjLookupQuery(List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  (SUM(?triples) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
					+ "\n}";
		return queryString;
	}
	
	
	public static String getSbj_ObjLookupQuery(List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  (SUM(?triples * 1/?ssel * 1/osel) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
				+ "		   \n?s ds:totalSbj ?ssel ."
				+ "		   \n?s ds:totalObj ?osel ."
						+ "\n}";
		return queryString;
	}
	
	public static String getSbjLookupQuery(List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  (SUM(?triples * 1/?sel) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
				+ "		   \n?s ds:totalSbj ?sel ."
						+ "\n}";
		return queryString;
	}
	
	public static String getObjLookupQuery(String p,List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  (SUM(?triples * 1/?sel) AS ?card) "
				+ " WHERE { \n" + union 
				+ "        \n?s ds:totalTriples ?triples . "        
				+ "		   \n?s ds:totalObj ?sel ."
						+ "\n}";
		return queryString;
	}

		public static String getPred_SbjLookupQuery(String p,List<StatementSource> stmtSrces) {
			String union = getEndpointUnion(stmtSrces);
			String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
					+ "SELECT  (SUM(?triples * ?sel) AS ?card) "
					+ " WHERE { \n" + union 
					+ " 	   \n?s ds:capability ?cap . "
					+ "		   \n?cap ds:predicate <" + p + "> ."
					+ "        \n?cap ds:triples ?triples . "
					+ "        \n?cap ds:avgSbjSelectivity ?sel ."
							+ "\n}";
			return queryString;
		}
	

	private static long executeQuery(RepositoryConnection conn, String queryString) {
		long results = 0;
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		//System.out.println(queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		try {
			while(result.hasNext())
			{
				results = (long) Double.parseDouble(result.next().getValue("card").stringValue());
			}
		} finally {
			result.close();
		}
		return results;
	}
	
	public static String getTotalTripleCountQuery(List<StatementSource> stmtSrces) {
		
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT (SUM(?totalTriples) AS ?card) "
				+ " WHERE { \n" + union 
				+ " 	   \n?s ds:totalTriples ?totalTriples . "
						+ "\n}";
		return queryString;
	}
	
	public static String getPred_ObjLookupQuery(String p,List<StatementSource> stmtSrces) {
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  (SUM(?triples * ?sel) AS ?card) "
				+ " WHERE { \n" + union 
				+ " 	   \n?s ds:capability ?cap . "
				+ "		   \n?cap ds:predicate <" + p + "> ."
				+ "        \n?cap ds:triples ?triples . "
				+ "        \n?cap ds:avgObjSelectivity ?sel ."
						+ "\n}";
		return queryString;
	}
	public static String getPredLookupQuery(String p,List<StatementSource> stmtSrces) {
	
		String union = getEndpointUnion(stmtSrces);
		String queryString = "Prefix ds:<http://aksw.org/quetsal/> \n"
				+ "SELECT  (SUM(?triples) AS ?card)"
				+ " WHERE { \n" + union 
				+ " 	   \n?s ds:capability ?cap . "
				+ "		   \n?cap ds:predicate <" + p + "> ."
				+ "        \n?cap ds:triples ?triples ."
						+ "\n}";
		return queryString;
	}

	private static String getEndpointUnion(List<StatementSource> stmtSrces) {
		String union = "";
		for (StatementSource s : stmtSrces)
		{
			if (union.equals(""))
				union= "{ ?s ds:url  <"+ s.getEndpointID().replace("sparql_", "http://").replace("_", "/") +"> . }" ;
			else
				union= union+ "\n UNION \n{  ?s ds:url  <"+ s.getEndpointID().replace("sparql_", "http://").replace("_", "/") +"> . }" ;
		}
		return union;
	}

	public static boolean boundPredicate(StatementPattern stmt) {
		return stmt.getPredicateVar().getValue() != null;
	}
	
	public static boolean boundSubject(StatementPattern stmt) {
		return stmt.getSubjectVar().getValue() != null;
	}
	
	public static boolean boundObject(StatementPattern stmt) {
		return stmt.getObjectVar().getValue() != null;
	}

}
