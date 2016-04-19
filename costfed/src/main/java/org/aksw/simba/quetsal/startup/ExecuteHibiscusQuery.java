package org.aksw.simba.quetsal.startup;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.simba.quetsal.core.HibiscusSourceSelection;
import org.aksw.simba.quetsal.core.QueryRewriting;
import org.aksw.sparql.query.algebra.helpers.BGPGroupGenerator;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

import com.fluidops.fedx.FedX;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
/**
 * Execute Query
 * @author Saleem
 *
 */
public class ExecuteHibiscusQuery {
	public static void main(String[] args) throws Exception {
		long strtTime = System.currentTimeMillis();
		String FedSummaries = "summaries/FedBench-HiBISCus.n3";
       //  String FedSummaries = "C://slices/Linked-SQ-DBpedia-Aidan.ttl";
		QuetzalConfig.Mode mode = QuetzalConfig.Mode.ASK_DOMINANT;;  //{ASK_DOMINANT, INDEX_DOMINANT}
		double commonPredThreshold = 0.33 ;  //considered a predicate as common predicate if it is presenet in 33% available data sources
		QuetzalConfig.initialize(FedSummaries, mode, commonPredThreshold);  // must call this function only one time at the start to load configuration information. Please specify the FedSum mode. 
		System.out.println("One time configuration loading time : "+ (System.currentTimeMillis()-strtTime));
		FedX fed = FederationManager.getInstance().getFederation();
		List<Endpoint> members = fed.getMembers();
		Cache cache =FederationManager.getInstance().getCache();
		List<String> queries = Queries.getFedBenchQueries();
		SPARQLRepository repo = new SPARQLRepository(members.get(0).getEndpoint());
		repo.initialize();
		int tpsrces = 0; 
		int count = 0;
		for (String query : queries)
		{
			System.out.println("-------------------------------------\n"+query);
			long startTime = System.currentTimeMillis();
			HibiscusSourceSelection sourceSelection = new HibiscusSourceSelection(members, cache, new QueryInfo(query, null));
			SPARQLParser parser = new SPARQLParser();
		    ParsedQuery parsedQuery = parser.parseQuery(query, null);
			List<List<StatementPattern>> bgpGroups = BGPGroupGenerator.generateBgpGroups(parsedQuery);
			sourceSelection.performSourceSelection(bgpGroups);
			Map<StatementPattern, List<StatementSource>> stmtToSources = sourceSelection.getStmtToSources();
			//  System.out.println(DNFgrps)
			System.out.println("Source selection exe time (ms): "+ (System.currentTimeMillis()-startTime));
                         int srces = 0;
						for (StatementPattern stmt : stmtToSources.keySet()) 
						{
							tpsrces = tpsrces+ stmtToSources.get(stmt).size();
							srces = srces + stmtToSources.get(stmt).size();
						
							//System.out.println("-----------\n"+stmt);
							//System.out.println(stmtToSources.get(stmt));
						}
			System.out.println("Total Triple Pattern-wise sources selected: " +srces);

	      //  count =  executeQuery(query,bgpGroups,stmtToSources,repo);
			System.out.println(": Query execution time (msec):"+ (System.currentTimeMillis()-startTime));
			System.out.println("Total results: " + count);
			Thread.sleep(1000);

		}	
		System.out.println("NetTriple pattern-wise selected sources after step 2 of HIBISCuS source selection : "+ tpsrces);
		FederationManager.getInstance().shutDown();
		System.exit(0);
	}
/**
 * Execute query and return the number of results
 * @param query SPARQL 	query
 * @param bgpGroups BGPs
 * @param stmtToSources Triple Pattern to sources
 * @param repo  repository
 * @return Number of results
 * @throws RepositoryException
 * @throws MalformedQueryException
 * @throws QueryEvaluationException
 */
	public static int executeQuery(String query, HashMap<Integer, List<StatementPattern>> bgpGroups, Map<StatementPattern, List<StatementSource>> stmtToSources, SPARQLRepository repo) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		String newQuery = QueryRewriting.doQueryRewriting(query,bgpGroups,stmtToSources);
		//	System.out.println(newQuery);
			TupleQuery tupleQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, newQuery); 
			int count = 0;
			TupleQueryResult result = tupleQuery.evaluate();
			while(result.hasNext())
			{
				//System.out.println(result.next());
				result.next();
				count++;
			}

		return count;
	}

	@SuppressWarnings("unused")
	private static void printParseQuery(String query) throws MalformedQueryException {
		SPARQLParserFactory factory = new SPARQLParserFactory();
		QueryParser parser = factory.getParser();
		ParsedQuery parsedQuery = parser.parseQuery(query, null);
		System.out.println(parsedQuery.toString());


	}

	/**
	 * Load query string from file
	 * @param qryFile Query File
	 * @return query Query string
	 * @throws IOException IO exceptions
	 */
	public static String  getQuery(File qryFile) throws IOException {
		String query = "" ; 
		BufferedReader br = new BufferedReader(new FileReader(qryFile));
		String line;
		while ((line = br.readLine()) != null)
		{
			query = query+line+"\n";
		}
		br.close();
		return query;
	}
}
