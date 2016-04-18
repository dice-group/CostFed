package org.aksw.simba.quetsal.startup;

//import  org.openrdf..queryrender.sparql.SPARQLQueryRenderer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.simba.quetsal.core.Cardinality;
import org.aksw.simba.quetsal.core.QueryRewriting;
import org.aksw.simba.quetsal.core.SourceRanking;
import org.aksw.simba.quetsal.core.TBSSSourceSelection;
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
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
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
public class ExecuteTBSSQuery {
	public static void main(String[] args) throws Exception {
		//queryRendererTest();		
		long strtTime = System.currentTimeMillis();
		//String theFedSummaries = "summaries/quetsal-Fedbench-b4.n3";
		String theFedSummaries = "summaries/s0.n3";
		//String FedSummaries = "C://slices/Linked-SQ-DBpedia-Aidan.ttl";
		QuetzalConfig.Mode mode = QuetzalConfig.Mode.ASK_DOMINANT;;  //{ASK_DOMINANT, INDEX_DOMINANT}
		double commonPredThreshold = 0.33 ;  //considered a predicate as common predicate if it is presenet in 33% available data sources
		QuetzalConfig.initialize(theFedSummaries, mode, commonPredThreshold);  // must call this function only one time at the start to load configuration information. Please specify the FedSum mode. 
		System.out.println("One time configuration loading time : "+ (System.currentTimeMillis()-strtTime));
		FedX fed = FederationManager.getInstance().getFederation();
		List<Endpoint> members = fed.getMembers();
		Cache cache = FederationManager.getInstance().getCache();
		List<String> queries = Queries.getFedBenchQueries();
		//List<String> queries =  getQueriesFromDir("queries/");
		SPARQLRepository repo = new SPARQLRepository(members.get(0).getEndpoint());
		repo.initialize();
		int tpsrces = 0; 
		int count = 0;
		//int k = 3;  top-k source selection
		for (String query : queries)
		{
			System.out.println("-------------------------------------\n"+query);
			long startTime = System.currentTimeMillis();
			TBSSSourceSelection sourceSelection = new TBSSSourceSelection(members, cache, new QueryInfo(query, null));
			SPARQLParser parser = new SPARQLParser();
			ParsedQuery parsedQuery = parser.parseQuery(query, null);
			//SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();  
			//query =  renderer.render(parsedQuery);
			
			List<List<StatementPattern>> bgpGroups = BGPGroupGenerator.generateBgpGroups(parsedQuery);
			sourceSelection.performSourceSelection(bgpGroups);
			Map<StatementPattern, List<StatementSource>> stmtToSources = sourceSelection.getStmtToSources();
			//Map<StatementPattern, List<StatementSource>> rankedStmtToSources =  new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
			//  System.out.println(DNFgrps)
			System.out.println("Source selection exe time (ms): "+ (System.currentTimeMillis()-startTime));
          // long rnktime = System.currentTimeMillis();
			for (StatementPattern stmt : stmtToSources.keySet()) 
			{
				tpsrces = tpsrces+ stmtToSources.get(stmt).size();
				System.out.println("-----------\n"+stmt);
				//System.out.println("\n--Random-\n"+stmtToSources.get(stmt));
				System.out.println("Cardinaltiy: " +Cardinality.getTriplePatternCardinality(stmt, stmtToSources.get(stmt)));
			//List<StatementSource>	rankedSrces = SourceRanking.getRankedTriplePatternSources(stmt, stmtToSources.get(stmt), k);
			//rankedStmtToSources.put(stmt, rankedSrces) ;
			//System.out.println("\n--Ranked-\n"+rankedSrces);
			//tpsrces = tpsrces+ rankedStmtToSources.get(stmt).size();
			}
			//System.out.println("Source Ranking time (msec):"+ (System.currentTimeMillis()-rnktime));
		//	count =  executeQuery(query,bgpGroups,rankedStmtToSources,repo);    //You can uncomment this if you want to execute the query as well. 
		//	System.out.println(": Query execution time (msec):"+ (System.currentTimeMillis()-startTime));
		//	System.out.println("Total results: " + count);
			System.out.println(count + "\t" + (System.currentTimeMillis()-startTime));
			Thread.sleep(1000);

		}	
		System.out.println("NetTriple pattern-wise selected sources: "+ tpsrces);
		FederationManager.getInstance().shutDown();
		System.exit(0);
	}

	@SuppressWarnings("unused")
	private static void queryRendererTest() throws Exception {
		String queryStr = "PREFIX abc: <http://aksw.org/> SELECT * { SERVICE SILENT <http://foo> { abc:pqr ?p ?o } }";
		SPARQLParser parser = new SPARQLParser();
		ParsedQuery query1 = parser.parseQuery(queryStr, null);
		System.out.println(query1);
		SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();  
		String  roundtrip =  renderer.render(query1);
		System.out.println("Roundtrip: " + roundtrip);

	}
	@SuppressWarnings("unused")
	private static List<String> getQueriesFromDir(String inputDirectory) throws IOException {
		List<String> queries = new ArrayList<String> ();
		File dir = new File(inputDirectory);
		File[] listOfQueries = dir.listFiles();
		for (File query : listOfQueries)
		{
			String queryStry = getQuery(query);	
			queries.add(queryStry);
		}
		return queries;
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
			System.out.println(newQuery);
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
