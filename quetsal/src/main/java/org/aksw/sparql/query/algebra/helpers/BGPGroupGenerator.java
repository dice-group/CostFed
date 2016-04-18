package org.aksw.sparql.query.algebra.helpers;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

/**
 * Generate basic graph patterns also called Disjunctive Normal Form (DNF) groups from a SPARQL query
 * @author Saleem
 *
 */
public class BGPGroupGenerator 
{
	/**
	 * Generate BGP groups from a SPARQL query
	 * @param parsedQuery TupleExpr of the SPARQL query
	 * @return DNFGrps Map of DNF groups 
	 */
	public static List<List<StatementPattern>> generateBgpGroups(String strQuery)
	{
		SPARQLParser parser = new SPARQLParser();
		ParsedQuery parsedQuery = parser.parseQuery(strQuery, null);
		return generateBgpGroups(parsedQuery);
	}
	
	public static List<List<StatementPattern>> generateBgpGroups(ParsedQuery parsedQuery)
	{
		List<List<StatementPattern>> bgpGrps = new ArrayList<List<StatementPattern>>();
		TupleExpr query = parsedQuery.getTupleExpr();
		// collect all basic graph patterns
		for (TupleExpr bgp : BasicGraphPatternExtractor.process(query)) {
			List<StatementPattern> patterns = StatementPatternCollector.process(bgp);	
			bgpGrps.add(patterns);
		}
		
		return bgpGrps;
	}
}
