package org.aksw.simba.quetsal.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.aksw.simba.quetsal.datastructues.Trie;
import org.aksw.simba.quetsal.datastructues.TrieNode;
import org.aksw.simba.quetsal.synopsis.ArrayCollection;
import org.aksw.simba.quetsal.synopsis.Collection;
import org.aksw.simba.quetsal.synopsis.MIPsynopsis;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;


/**
 *  Generate Quetzal-TBSS Summaries for a set of federation members (SPARQL endpoints)
 *  @author saleem
 */
public class TBSSSummariesGenerator {
	static Logger log = Logger.getLogger(TBSSSummariesGenerator.class);
	
	public BufferedWriter bwr;
	//public double distinctSbj;
	//public long trplCount ;
	
	/**
	 * initialize input information for data summaries generation
	 * @param location Directory location of the resulting FedSummaries file (i.e. location/FedSum.n3)
	 * @throws IOException IO Exceptions
	 */
	public TBSSSummariesGenerator(String location) throws IOException 
	{
		bwr = new BufferedWriter(new FileWriter(new File(location))); //--name/location where the summaries file will be stored
		bwr.append("@prefix ds:<http://aksw.org/quetsal/> .");
		bwr.newLine();
	}
	
	public static void main(String[] args) throws IOException
	{
		//String host = "10.15.0.144";
		//String host = "192.168.0.145";
		//String host = "ws24348.avicomp.com";
		String host = "localhost";
		///*
		List<String> endpoints = Arrays.asList(
			 "http://" + host + ":8890/sparql",
			 "http://" + host + ":8891/sparql",
			 "http://" + host + ":8892/sparql",
			 "http://" + host + ":8893/sparql",
			 "http://" + host + ":8894/sparql",
			 "http://" + host + ":8895/sparql",
			 "http://" + host + ":8896/sparql",
			 "http://" + host + ":8897/sparql",
			 "http://" + host + ":8898/sparql"
			 
			 , "http://" + host + ":8887/sparql"
			 , "http://" + host + ":8888/sparql"
			 , "http://" + host + ":8889/sparql"
			 , "http://" + host + ":8899/sparql"
		);
//*/
		//List<String> endpoints = Arrays.asList("http://" + host + ":8890/sparql", "http://" + host + ":8890/sparql");
		String outputFile = "summaries/sum-localhost.n3";
		int branchLimit = 4;
		
	//	String namedGraph = "http://aksw.org/fedbench/";  //can be null. in that case all graph will be considered 
		String namedGraph = null;
		TBSSSummariesGenerator generator = new TBSSSummariesGenerator(outputFile);
		long startTime = System.currentTimeMillis();
		generator.generateSummaries(endpoints, namedGraph, branchLimit);
		//generator.generateDAWSummaries(endpoints, namedGraph, branchLimit, 0.5);
		log.info("Data Summaries Generation Time (min): "+ (double)(System.currentTimeMillis() - startTime) / (1000 * 60));
		log.info("Data Summaries are secessfully stored at "+ outputFile);

		//	outputFile = "summaries\\quetzal-b2.n3";
		//	generator = new SummariesGenerator(outputFile);
		//	startTime = System.currentTimeMillis();
		//	branchLimit =2;
		//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
		//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
		//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
		//	
		//	outputFile = "summaries\\quetzal-b5.n3";
		//	generator = new SummariesGenerator(outputFile);
		//	startTime = System.currentTimeMillis();
		//	branchLimit =5;
		//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
		//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
		//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
		//	
		//	outputFile = "summaries\\quetzal-b10.n3";
		//	generator = new SummariesGenerator(outputFile);
		//	startTime = System.currentTimeMillis();
		//	branchLimit =10;
		//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
		//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
		//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
	}
	
	ExecutorService executorService;
	/**
	 * Build Quetzal data summaries for the given list of SPARQL endpoints
	 * @param endpoints List of SPARQL endpoints url
	 * @param graph Named graph. Can be null. In this case all named graphs will be considered for Quetzal summaries
	 * @param branchLimit Branching limit
	 * @throws IOException IO Error
	 */
	public void generateSummaries(List<String> endpoints, String graph, int branchLimit) throws IOException
	{
		executorService = Executors.newFixedThreadPool(10);
		List<Future<?>> flist = new ArrayList<Future<?>>();
		AtomicInteger dsnum = new AtomicInteger(0);
		for (String endpoint : endpoints)
		{
			Future<?> future = executorService.submit(new Runnable() {
			    public void run() {
			    	try {
				    	String sum = generateSummary(endpoint, graph, branchLimit, dsnum.incrementAndGet());
				    	
				    	synchronized(bwr) {
				    		try {
				    			bwr.append(sum);
				    			bwr.flush();
				    		} catch (Exception e) {
				    			log.error(e);
				    		}
				    	}
			    	} catch (Exception e) {
			    		log.error(e);
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
		bwr.close();
	}
	
	public String generateSummary(String endpoint, String graph, int branchLimit, int dsnum) throws Exception
	{
		long totalTrpl = 0;
		//long totalSbj = 0;
		//long totalObj = 0;
		long totalSbj = getDistinctSubjectCount(endpoint);
		long totalObj = getDistinctObjectCount(endpoint);
		
		StringBuilder sb = new StringBuilder();
		
		List<String> lstPred = getPredicates(endpoint, graph);
		log.info("total distinct predicates: "+ lstPred.size() + " for endpoint: " + endpoint);
		
		sb.append("#---------------------"+endpoint+" Summaries-------------------------------\n");
		sb.append("[] a ds:Service ;\n");
		sb.append("     ds:url   <"+endpoint+"> ;\n");
		for (int i = 0; i < lstPred.size(); i++)
		{
			log.info((i+1)+" in progress: " + lstPred.get(i) + ", endpoint: " + endpoint);
			sb.append("     ds:capability\n");
			sb.append("         [\n");
			sb.append("           ds:predicate  <" + lstPred.get(i) + "> ;");
			long distinctSbj = writeSbjPrefixes(lstPred.get(i), endpoint, graph, branchLimit, sb);
			long tripleCount = getTripleCount(lstPred.get(i), endpoint);
			if (distinctSbj == 0) {
				distinctSbj = tripleCount;
			}
			sb.append("\n           ds:avgSbjSelectivity  " + (1 / (double)distinctSbj) + " ;");
			
			writeObjPrefixes(lstPred.get(i), endpoint, graph, branchLimit, sb);
			double distinctObj = getObj(lstPred.get(i), endpoint);
			sb.append("\n           ds:avgObjSelectivity  " + (1 / distinctObj) + " ;\n");

			sb.append("           ds:triples    " + tripleCount + " ;\n");
			sb.append("         ] ;\n");
			
			//totalSbj += distinctSbj;
			//totalObj += (long)distinctObj ;
			totalTrpl += tripleCount;
		}
		

		sb.append("     ds:totalSbj "+totalSbj+" ; \n");  // this is not representing the actual number of distinct subjects in a datasets since the same subject URI can be shaared by more than one predicate. but we keep this to save time.  
		sb.append("     ds:totalObj "+totalObj+" ; \n");  
		sb.append("     ds:totalTriples "+totalTrpl+" ; \n");
		sb.append("             .\n");

		//     bw.append("     sd:totalTriples \""+totalTrpl+"\" ;");
		//bw.append("#---------End---------");
		//bw.close();
		return sb.toString();
	}

	/**
	 * Build Duplicate-aware Quetzal data summaries for the given list of SPARQL endpoints
	 * @param endpoints List of SPARQL endpoints url
	 * @param graph Named graph. Can be null. In this case all named graphs will be considered for Quetzal summaries
	 * @param branchLimit Branching limit
	 * @param d Size of MIPS vector can be fixed, e.g., 64 or in percentage, e.g. 0.10 mean the size is the 10% of the ds:triples in dataset capabilities 
	 * @throws IOException Io Error
	 * @throws QueryEvaluationException Query Execution Error 
	 * @throws MalformedQueryException  Memory Error
	 * @throws RepositoryException  Repository Error
	 */
	public void generateDAWSummaries(String endpoint, String graph, int branchLimit, double d, int dsnum) throws IOException
	{
		StringBuilder sb = new StringBuilder();

		long totalTrpl = 0, totalSbj = 0, totalObj = 0;
		String aMIPsV = "";
		List<String> lstPred = getPredicates(endpoint, graph);
		log.info("total distinct predicates: "+ lstPred.size() + " for endpoint: " + endpoint);
		sb.append("#---------------------" + endpoint + " Summaries-------------------------------\n");

		sb.append("[] a ds:Service ;\n");
		sb.append("     ds:url   <"+endpoint+"> ;\n");
		for (int i = 0; i < lstPred.size(); i++)
		{
			log.info((i+1)+" in progress: " + lstPred.get(i) + ", endpoint: " + endpoint);
			sb.append("     ds:capability\n");
			sb.append("         [\n");
			sb.append("           ds:predicate  <"+lstPred.get(i)+"> ;\n");
			aMIPsV = getMIPsV(lstPred.get(i), endpoint, d);
			sb.append("\n           ds:MIPv       \"" + aMIPsV + "\" ;");
			long distinctSbj = writeSbjPrefixes(lstPred.get(i), endpoint, graph, branchLimit, sb);
			long trpleCount = getTripleCount(lstPred.get(i),endpoint);
			if (distinctSbj == 0) {
				distinctSbj = trpleCount;
			}
			sb.append("\n           ds:avgSbjSelectivity  "+ (1 / (double)distinctSbj)+" ;");
			totalSbj = (long) (totalSbj+distinctSbj);
			writeObjPrefixes(lstPred.get(i), endpoint, graph, branchLimit, sb);
			double distinctObj = getObj(lstPred.get(i),endpoint);
			sb.append("\n           ds:avgObjSelectivity  "+ (1 / distinctObj)+" ;\n");
			totalObj = (long) (totalObj+distinctObj);
			sb.append("           ds:triples    "+trpleCount+" ;");
			sb.append("         ] ;\n");
			totalTrpl = totalTrpl+trpleCount;
		}
		sb.append("     ds:totalSbj "+totalSbj+" ; \n");  // this is not representing the actual number of distinct subjects in a datasets since the same subject URI can be shaared by more than one predicate. but we keep this to save time.  
		sb.append("     ds:totalObj "+totalObj+" ; \n");  
		sb.append("     ds:totalTriples "+totalTrpl+" ; \n\n");
		sb.append("             .\n");
		//     bw.append("     sd:totalTriples \""+totalTrpl+"\" ;");
	}
	
	/**
	 * Get total number of triple for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	public static double getObj(String pred, String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		String strQuery = "SELECT  (COUNT(DISTINCT ?o) AS ?objs) " + // 
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
			return Long.parseLong(rs.next().getValue("objs").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	
	/**
	 * Get total number of distinct subjects of a dataset
	 * @return count 
	 */
	public static Long getSubjectsCount(String endpoint) {
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
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	public static Long getObjectsCount(String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		long count = 0;
		String strQuery = "SELECT  (COUNT(DISTINCT ?o) AS ?objts) " + // 
				"WHERE " +
				"{" +

	       		"?s ?p ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult rs = query.evaluate();
		while( rs.hasNext() ) 
		{
			count = Long.parseLong(rs.next().getValue("objts").stringValue());

		}
		return count;
	}
	/**
	 * Get total number of triple for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 */
	public static Long getTripleCount(String pred, String endpoint) {
		String strQuery = "SELECT  (COUNT(*) AS ?triples) " + // 
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
	
	public static Long getDistinctSubjectCount(String endpoint) {
		String strQuery = "SELECT  (COUNT(distinct ?s) AS ?triples) " + // 
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
			return Long.parseLong(rs.next().getValue("triples").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
	}
	
	public static Long getDistinctObjectCount(String endpoint) {
		String strQuery = "SELECT  (COUNT(distinct ?o) AS ?triples) " + // 
				"WHERE " +
				"{" +
	       		"?s ?p ?o " +
				"FILTER isIRI(?o)" +
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
	
	/**
	 * Get all leaf branches of a the each of the URI
	 * @param authGroups List of URIs grouped by same authority
	 * @param branchLimit Branching limit
	 * @return URIs
	 */
	public static SortedSet<String> getAllBranchingURIs(Map<String, List<String>> authGroups, int branchLimit) {
		SortedSet<String> uris = new TreeSet<String>();
		for (List<String> uRIs : authGroups.values())
		{
			//if(auth.equals("http://sdow2008.semanticweb.org/"));
			//  	System.out.println(uRIs);
			TrieNode root = Trie.constructTrie(uRIs, branchLimit);
			uris.addAll(Trie.getRoot2StopingNodePaths(root, branchLimit)); // union
			//System.out.println("Authority: "+auth+", LCP:"+lcp);
		}
		return uris;
	}
	
	/**
	 * Write all the distinct subject prefixes  for triples with predicate p. 
	 * @param predicate  Predicate
	 * @param endpoint Endpoint URL
	 * @param graph named Graph
	 * @param branchLimit Branching Limit for Trie
	 * @throws IOException  IO Error
	 */
	public long writeSbjPrefixes(String predicate, String endpoint, String graph, int branchLimit, StringBuilder sb) throws IOException {
		long distinctSbj = 0;
		
		Map<String, List<String>> authGroups = new HashMap<String, List<String>>();
		String strQuery = getSbjAuthorityQuery(predicate, graph);

		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();

		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult res = query.evaluate();
			int rsCount = 0; 
			while (res.hasNext()) 
			{
				String curSbj = res.next().getValue("s").toString();
				rsCount++;
				//System.out.println(curSbj);
				String[] sbjPrts = curSbj.split("/");
				if ((sbjPrts.length > 2))
				{
					String sbjAuth = sbjPrts[0] + "//" + sbjPrts[2];
					//if(!sbjAuthorities.contains(sbjAuth))
					//	sbjAuthorities.add(sbjAuth);
					//---------
					List<String> theURIs = authGroups.get(sbjAuth);
					if (null == theURIs) {
						theURIs = new ArrayList<String>();
						authGroups.put(sbjAuth, theURIs);
					}
					theURIs.add(curSbj);
				} else {
					log.warn("Subject <" + curSbj + "> is not a valid URI. Subject prefix ignored.");
				}
			}
			distinctSbj = rsCount;
		} finally {
			conn.close();
			repo.shutDown();
		}

		//sbjAuthorities = Prefix.getLCPs(authGroups);

		String[] sbjAuthorities = getAllBranchingURIs(authGroups, branchLimit).toArray(new String[]{});
		if (sbjAuthorities.length > 0)
		{
			sb.append("\n           ds:sbjPrefix ");
			String prevAuth = "" + Character.MAX_VALUE;
			for (int authority = 0; authority < sbjAuthorities.length; authority++)
			{
				String strAuth = sbjAuthorities[authority];
				if (strAuth.startsWith(prevAuth)) {
					continue;
				}
				prevAuth = strAuth;
				if (authority == sbjAuthorities.length - 1) {
					sb.append("<" + strAuth.replace(" ", "") + "> ; ");
				} else {
					sb.append("<" + strAuth.replace(" ", "") + ">, ");
				}
			}
		}
		return distinctSbj;
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
	 * Write all the distinct object authorities having predicate p. 
	 * @param predicate  Predicate
	 * @param endpoint Endpoint URL
	 * @param graph named Graph
	 * @param branchLimit Branching limit for Trie
	 * @throws IOException  IO Error
	 */
	public void writeObjPrefixes(String predicate, String endpoint, String graph, int branchLimit, StringBuilder sb) throws IOException {
		SortedSet<String> objAuthorities = new TreeSet<String>();
		Map<String, List<String>> authGroups = new HashMap<String, List<String>>();
		String strQuery = getObjAuthorityQury(predicate, graph);
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		try {
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			TupleQueryResult res = query.evaluate();
			while (res.hasNext()) 
			{
				if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					objAuthorities.add(res.next().getValue("o").toString());
				} else {
					String anObj = res.next().getValue("o").toString();
					// System.out.println("obj:" + anObj);
					String[] objPrts = anObj.split("/");
					if (objPrts.length > 2)
					{
						String objAuth = objPrts[0] + "//" + objPrts[2];
						//if(!objAuthorities.contains(objAuth))
						//	objAuthorities.add(objAuth);  
						List<String> theURIs = authGroups.get(objAuth);
						if (null == theURIs) {
							theURIs = new ArrayList<String>();
							authGroups.put(objAuth, theURIs);
						}
						theURIs.add(anObj);
					} else {
						log.warn("Problem with object <" + anObj + ">. Object authorithy ignored for that uri");
					}
				}
			}
		} finally {
			conn.close();
			repo.shutDown();
		}

		//objAuthorities.addAll(Prefix.getLCPs(authGroups));
		objAuthorities.addAll(getAllBranchingURIs(authGroups, branchLimit));
		if (!objAuthorities.isEmpty())
		{
			sb.append("\n           ds:objPrefix ");
			String[] objAuthoritiesArr = objAuthorities.toArray(new String[]{});
			String prevAuth = "" + Character.MAX_VALUE;
			for (int authority = 0; authority < objAuthorities.size(); authority++)
			{
				String strAuth = objAuthoritiesArr[authority];
				if (strAuth.startsWith(prevAuth)) {
					continue;
				}
				prevAuth = strAuth;
				if (authority == objAuthorities.size() - 1) {
					sb.append("<" + strAuth.replace(" ", "") + "> ; ");
				} else {
					sb.append("<"+ strAuth.replace(" ", "")+ ">, ");
				}
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
	 * @param graph Named graph
	 * @return query Required SPARQL query
	 */
	public String getObjAuthorityQury(String predicate, String graph) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT DISTINCT ?o");
		if (null != graph) {
			sb.append(" FROM <");
			sb.append(graph);
			sb.append(">");
		}
		sb.append(" WHERE { ?s <");
		sb.append(predicate);
		sb.append("> ?o. FILTER(isURI(?o)) }");

		return sb.toString();
	}
	
	//-----------------------Return Mean wise independent permutation vector (MIPsV) for a predicate-------------	
	/**
	 * Generate Mean wise independent permutation vector (MIPsV) for a predicate
	 * @param pred Predicate name
	 * @param endpoint SPARQL endpoint URL
	 * @param d MipsVector Size can be fixed  e.g. 64 or in percentage e.g., 0.10 means 10%
	 * @return MIPsV Mips Vector
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	private String getMIPsV(String pred, String endpoint, double d) throws IOException
	{
		ArrayList<Long> idsVector = new ArrayList<Long>() ;
		String MIPsV = "";
		String strQuery = getMIPsVQury(pred);
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult rs = query.evaluate();
		while( rs.hasNext() ) 
		{
			BindingSet bset = rs.next();
			String sbj_obj = bset.getBinding("s").getValue().toString().concat(bset.getBinding("o").getValue().toString());
			idsVector.add((long) (sbj_obj.hashCode()));
		}
		long trplCount = idsVector.size();
		Collection c = new ArrayCollection(idsVector);
		MIPsynopsis synMIPs = null;
		if (d >= 1) {
			synMIPs = new MIPsynopsis(c, (int)d, 242); 
		} else {
		    synMIPs = new MIPsynopsis(c, (int)(Math.ceil(trplCount*d)), 242); 
		}
		long[] minValues = synMIPs.minValues;
		for (int i=0; i<minValues.length; i++)
			MIPsV = MIPsV.concat(minValues[i]+" ");

		return MIPsV.trim();
	}
	
	//--------------------------------------------------------------------------
	/**
	 * Get query for MIPs Vectory generation
	 * @param pred Predicate name for which MIPs vector is required
	 */
	private static String getMIPsVQury(String pred) 
	{
		String MIPsVQuery = "SELECT   ?s  ?o " + // 
				"WHERE " +
				"{" +

				               		"?s <"+pred+"> ?o " +
				               		"} " ;
		return MIPsVQuery;
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
		String strQuery = getPredQury(graph);
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
	private static String getPredQury(String graph) {
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