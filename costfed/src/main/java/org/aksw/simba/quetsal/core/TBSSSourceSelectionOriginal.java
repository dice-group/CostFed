package org.aksw.simba.quetsal.core;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.simba.quetsal.datastructues.HyperGraph.HyperEdge;
import org.aksw.simba.quetsal.datastructues.HyperGraph.Vertex;
import org.aksw.simba.quetsal.datastructues.Trie;
import org.aksw.simba.quetsal.datastructues.TrieNode;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.EmptyStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.cache.CacheEntry;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.optimizer.SourceSelection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.SubQuery;
import com.fluidops.fedx.util.QueryStringUtil;
/**
 * Perform triple pattern-wise source selection using Quetsal Sumaries, cache and SPARQL ASK. 
 * @author Saleem
 *
 */
public class TBSSSourceSelectionOriginal extends SourceSelection {
	static Logger log = Logger.getLogger(TBSSSourceSelectionOriginal.class);
	
	public Map<HyperEdge, StatementPattern> hyperEdgeToStmt = new HashMap<HyperEdge,StatementPattern>(); //Hyper edges to Triple pattern Map
	public List<Map<String, Vertex>> theDNFHyperVertices = new ArrayList<Map<String, Vertex>>(); // Maps of vertices in different DNF hypergraphs

	///protected final List<StatementPattern> triplePatterns;
	
	/**
	 * Constructor
	 * @param endpoints set of endpoints url
	 * @param cache cache
	 * @param query SPARQL query
	 */
	public TBSSSourceSelectionOriginal(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		super(endpoints, cache, queryInfo);
	}
	
	public List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();

	private Vertex collectVertex(Var var, String label, Map<String, Vertex> v) {
		Vertex resultVertex = v.get(label);
		if (null == resultVertex) {
			resultVertex = new Vertex(var, label);
			v.put(label, resultVertex);
		}
		return resultVertex;
	}
	
	/**
	 * Perform triple pattern-wise source selection for the provided statement patterns of a SPARQL query using Quetzal Summaries, cache or remote ASK queries.
	 * Remote ASK queries are evaluated in parallel using the concurrency infrastructure of FedX. Note,
	 * that this method is blocking until every source is resolved.
	 * Recent SPARQL ASK operations are cached for future use.
	 * The statement patterns are replaced by appropriate annotations in this process.
	 * Hypergraphs are created in step 1 of source selection and are used in step 2 i.e. prunning of 
	 * triple pattern-wise selected sources. 
	 * @param bgpGroups BGPs
	 * @return
	 */
	@Override
	public void performSourceSelection(List<List<StatementPattern>> bgpGroups) 
	{
		// Map statements to their sources. Use synchronized access!
		stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
		
		long tp = 0;
		for (List<StatementPattern> stmts : bgpGroups)
		{
			Map<String, Vertex> v = new HashMap<String, Vertex>();   //--Set (in fact) of all vertices used in our hypergraph. each subject, predicate and object of a triple pattern is one node until it is repeated
			//    HashSet<HyperEdge> E = new HashSet<HyperEdge>();  //-- set of all hyperedges. an edge contains subject, predicate and object node of a triple pattern

			// for each statement determine the relevant sources. Note each  statement pattern is a hyperedge as depicted in step 1 source selection algo given in FedSum paper
			for (StatementPattern stmt : stmts) 
			{
				tp++;
				//cache.clear();
				stmtToSources.put(stmt, new ArrayList<StatementSource>());
				//--------
				String s = null, p = null, o = null, sa = null, oa = null;
				Vertex sbjVertex, predVertex, objVertex;
				
				if (stmt.getSubjectVar().getValue() != null)
				{
					s = stmt.getSubjectVar().getValue().stringValue();
					String[] sbjPrts = s.split("/");
					sa = sbjPrts[0] + "//" + sbjPrts[2];
					//-- add subjectVertex
					sbjVertex = collectVertex(stmt.getSubjectVar(), s, v);
				} else {
					sbjVertex = collectVertex(stmt.getSubjectVar(), stmt.getSubjectVar().getName(), v);
				}
				
				if (stmt.getPredicateVar().getValue() != null)
				{
					p = stmt.getPredicateVar().getValue().stringValue();
					predVertex = collectVertex(stmt.getPredicateVar(), p, v);
				} else {
					predVertex = collectVertex(stmt.getPredicateVar(), stmt.getPredicateVar().getName(), v);
				}
				
				if (stmt.getObjectVar().getValue() != null)
				{
					o = stmt.getObjectVar().getValue().stringValue();
					String[] objPrts = o.split("/");
					if (objPrts.length > 2) {     //add only URI
						oa = objPrts[0] + "//" + objPrts[2];
					}
					objVertex = collectVertex(stmt.getObjectVar(), o, v);
				} else {
					objVertex = collectVertex(stmt.getObjectVar(), stmt.getObjectVar().getName(), v);
				}
				
				//-------Step 1 of our source selection---i.e Triple pattern-wise source selection----
				if (QuetzalConfig.mode == QuetzalConfig.Mode.ASK_DOMINANT)   //---ASK_dominant algo
				{
					if (s == null && p == null && o == null)
					{
						for (Endpoint e : endpoints) 
							addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
					}
					else if (p != null)
					{
						if (p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && o != null)  
							lookupFedSumClass(stmt, p, o);
						else if (!QuetzalConfig.commonPredicates.contains(p) || (s == null && o == null))
							lookupFedSum(stmt, sa, p, oa);
						else
							cache_ASKselection(stmt);    	 
					} else {
						cache_ASKselection(stmt);
					}
				}
				else // Index_dominant source selection algo
				{
					if (s == null && p == null && o == null)
					{
						for (Endpoint e : endpoints) 
							addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
					}
					else if (s != null || p != null)
					{
						if ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p) && o != null)
							lookupFedSumClass(stmt, p, o);
						else
							lookupFedSum(stmt, sa, p, oa);
					}
					else
						cache_ASKselection(stmt);
				}
				//--------add hyperedges
				HyperEdge hEdge = new HyperEdge(sbjVertex, predVertex, objVertex);
				// E.add(hEdge);
				
				sbjVertex.outEdges.add(hEdge); predVertex.inEdges.add(hEdge); objVertex.inEdges.add(hEdge);
				hyperEdgeToStmt.put(hEdge, stmt);
			}
			theDNFHyperVertices.add(v);
		}
		// long askStartTime = System.currentTimeMillis();

		// if remote checks are necessary, execute them using the concurrency
		// infrastructure and block until everything is resolved
		if (remoteCheckTasks.size() > 0) {
			SourceSelectionExecutorWithLatch.run(this, remoteCheckTasks, cache);
		}

		if (log.isDebugEnabled()) {
			log.debug("Number of ASK request: " + remoteCheckTasks.size());
		}
		
		/////////////////////////////
		//System.out.println("Total Triple Pattern-wise selected sources in step 1 of Quetzal Source selection: " + triplePatternWiseSources);
		//System.out.println("step 1 time: "+ (System.currentTimeMillis()-step1Strt));
		//--------------------------------Step 2 of our source selection i.e. pruning selected sources in step 1 using hypergraphs--------------
		//		for(int DNFkey:DNFHyperVertices.keySet())
		//		{
		//			HashSet<Vertex> V = DNFHyperVertices.get(DNFkey);
		//			System.out.println("--------------new DNF Graph---------");
		//		 for (Vertex v:V)
		//		  System.out.println("vertex: "+v+" , InEdges: "+v.inEdges.size() + " outEdges: "+v.outEdges.size());
		//		}
		int triplePatternWiseSources = 0 ;
		for (Map.Entry<StatementPattern, List<StatementSource>> stmtentry : stmtToSources.entrySet()) {
			triplePatternWiseSources = triplePatternWiseSources + stmtentry.getValue().size();
		}
		
		if (triplePatternWiseSources > tp) {
			stmtToSources =  pruneSources(theDNFHyperVertices);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Total Triple pattern-wise selected sources: "+ triplePatternWiseSources);
			}
		}
		///////////////////////////////////////////////
		// long endTime = System.currentTimeMillis();
		//System.out.println("Ask queries Cost: Execution time(msec) : "+ (endTime-askStartTime));
		//triplePatternWiseSources = 0 ;
		for (Map.Entry<StatementPattern, List<StatementSource>> stmtentry : stmtToSources.entrySet()) {
			StatementPattern stmt = stmtentry.getKey();
			List<StatementSource> sources = stmtentry.getValue();
			//System.out.println("-----------\n"+stmt);
			//System.out.println(sources);
			//triplePatternWiseSources = triplePatternWiseSources + sources.size();
			// if more than one source -> StatementSourcePattern
			// exactly one source -> OwnedStatementSourcePattern
			// otherwise: No resource seems to provide results

			if (sources.size() > 1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
				for (StatementSource s : sources)
					stmtNode.addStatementSource(s);
				stmt.replaceWith(stmtNode);
			}

			else if (sources.size() == 1) {
				stmt.replaceWith(new ExclusiveStatement(stmt, sources.get(0), queryInfo));
			}

			else {
				if (log.isDebugEnabled()) {
					log.debug("Statement " + QueryStringUtil.toString(stmt) + " does not produce any results at the provided sources, replacing node with EmptyStatementPattern.");
				}
				stmt.replaceWith( new EmptyStatementPattern(stmt));
			}
		}


	}

//	/**
//	 * Retrieve a vertex having a specific label from a set of Vertrices
//	 * @param label Label of vertex to be retrieved
//	 * @param V Set of vertices
//	 * @return Vertex if exist otherwise null
//	 */
//	public Vertex getVertex(String label, Set<Vertex> vs) {
//		for (Vertex v : vs)
//		{
//			if (v.label.equals(label))
//				return v;
//		}
//		return null;
//	}
	
//	/**
//	 * Check if a  vertex already exists in set of all vertices
//	 * @param sbjVertex Subject Vertex
//	 * @param V Set of all vertices
//	 * @return value Boolean value
//	 */
//	public boolean vertexExist(Vertex sbjVertex, Set<Vertex> V) {
//		for (Vertex v : V)
//		{
//			if (sbjVertex.label.equals(v.label))
//				return true;
//		}
//		return false;
//	}
	/**
	 * Use cache and SPARQL ASK to perform relevant source selection for a statement pattern
	 * @param stmt statement pattern
	 */
	public void cache_ASKselection(StatementPattern stmt)
	{

		SubQuery q = new SubQuery(stmt);
		// check for each current federation member (cache or remote ASK)
		for (Endpoint e : endpoints)
		{
			StatementSourceAssurance a = cache.canProvideStatements(q, e);
			if (a == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) 
				addSource(stmt, new StatementSource(e.getId(), StatementSourceType.LOCAL));
			else if (a == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) 
				addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
			else if (a == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) 
				remoteCheckTasks.add(new CheckTaskPair(e, stmt));
		}
	}

	/**
	 * Search Quetzal index for the given triple pattern p with sbj authority and obj authority.
	 * Note: sa, oa can be null i.e. for unbound tuple 
	 * @param stmt Statement pattern	
	 * @param sa Subject authority
	 * @param p Predicate
	 * @param oa Object authority
	 */
	public void lookupFedSum(StatementPattern stmt, String sa, String p, String oa) {
		String  queryString = getFedSumLookupQuery(sa, p, oa) ;
		TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		//System.out.println(queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while(result.hasNext())
		{
			String endpoint = result.next().getValue("url").stringValue();
			String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
			//System.out.println(stmt.getPredicateVar().getValue() + " source: " + id);
			addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
		}
	}
	/**
	 * Get SPARQL query for index lookup
	 * @param sa Subject Authority
	 * @param p Predicate
	 * @param oa Object Authority
	 * @return queryString Query String
	 */
	public String getFedSumLookupQuery(String sa, String p, String oa) {
		String queryString = null;
		if (p != null)	
		{
			if (sa == null && oa == null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ " 		?s ds:capability ?cap . "
						+ "		   ?cap ds:predicate <" + p + ">.}"

						;
			}
			else if (sa != null && oa != null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ " 		?s ds:capability ?cap . "
						+ "		   ?cap ds:predicate <" + p + "> . "
						+ "?cap ds:sbjPrefix  ?sbjAuth . "
						+ "?cap ds:objPrefix  ?objAuth . "
						+ "FILTER REGEX(STR(?sbjAuth), \"" +sa + "\", \"i\") "
						+ "FILTER REGEX(STR(?objAuth), \"" +oa + "\", \"i\") "
						+ "}" ;	
			}
			else if (sa != null && oa == null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ "?s ds:capability ?cap . "
						+ "?cap ds:predicate <" + p + "> . "
						+ "?cap ds:sbjPrefix  ?sbjAuth ."
						+ "FILTER REGEX(STR(?sbjAuth), \"" +sa + "\", \"i\") "
						+ "}"
						;	
				//System.out.println(queryString);
			}
			else if (sa == null && oa != null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ "?s ds:capability ?cap . "
						+ "?cap ds:predicate <" + p + "> . "
						+ "?cap ds:objPrefix  ?objAuth . "
						+ "FILTER REGEX(STR(?objAuth), \"" +oa + "\", \"i\") "

								+ "}" ;	
				//	System.out.println(queryString);
			}
		}
		else
		{
			if (sa != null && oa != null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ " 		?s ds:capability ?cap . "
						+ "?cap ds:sbjPrefix  ?sbjAuth . "
						+ "?cap ds:objPrefix  ?objAuth . "
						+ "FILTER REGEX(STR(?sbjAuth), \"" + sa + "\", \"i\") "
						+ "FILTER REGEX(STR(?objAuth), \"" + oa + "\", \"i\") }";	
			}
			else if (sa != null && oa == null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ " 		?s ds:capability ?cap . "
						+ "?cap ds:sbjPrefix  ?sbjAuth ."
						+ "FILTER REGEX(STR(?sbjAuth), \"" +sa + "\", \"i\") "
						+ "}"
						;	
			}
			else if (sa == null && oa != null)
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?url "
						+ " WHERE {?s ds:url ?url . "
						+ " 		?s ds:capability ?cap . "
						+ "?cap ds:objPrefix  ?objAuth . "
						+ "FILTER REGEX(STR(?objAuth), \"" +oa + "\", \"i\") "

								+ "}" ;	
			}
		}

		return queryString;
	}
	
	/**
	 * Quetzal Index lookup for rdf:type and its its corresponding values
	 * @param p Predicate i.e. rdf:type
	 * @param o Predicate value
	 * @param stmt Statement Pattern
	 */
	public void lookupFedSumClass(StatementPattern stmt, String p, String o) {

		String  queryString = "Prefix ds:<http://aksw.org/quetsal/> "
				+ "SELECT  Distinct ?url "
				+ " WHERE {?s ds:url ?url. "
				+ " 		?s ds:capability ?cap. "
				+ "		   ?cap ds:predicate <" + p + ">."
				+ "?cap ds:objPrefix  <" + o + "> }" ;
		TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext())
		{
			String endpoint = result.next().getValue("url").stringValue();
			String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
			addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
		}
	}
	
	//------------------------------------------------------------
	/**
	 * Step 2 of Quetzal source selection. i.e. triple pattern-wise selected sources for hyperedge aka triple pattern
	 * @param dNFHyperVertices DNF groups (BGPs)of hypervertices
	 * @return Refine triple pattern-wise selected sources
	 */
	public Map<StatementPattern, List<StatementSource>> pruneSources(List<Map<String, Vertex>> dNFHyperVertices)
	{
		for (Map<String, Vertex> vs : dNFHyperVertices)
		{
			//System.out.println("--------------new DNF Graph---------");
			if (vs.size() > 3)  //---- only consider those DNF groups having at least 2 triple patterns
			{
				for (Vertex v : vs.values()) //vertex
				{
					Map<StatementPattern, Map<StatementSource, List<String>>> stmtToLstAuthorities = new HashMap<StatementPattern, Map<StatementSource, List<String>>>(); 
					List<String> authIntersectionSet = new ArrayList<String>();
					//---------------------------------------hybrid node-------------------------------------------------------------
					if ((v.inEdges.size() > 1 && !v.outEdges.isEmpty()) || (!v.inEdges.isEmpty() && v.outEdges.size() > 1)) 
					{
						//System.out.println(v.label + " is hybrid node");
						for(HyperEdge inEdge : v.inEdges) //has hyperedges or statement patterns
						{
							Map<StatementSource, List<String>> stmtSourceToAuthorities = new HashMap<StatementSource, List<String>> ();
							List<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt =  hyperEdgeToStmt.get(inEdge);
							for (StatementSource src : stmtToSources.get(stmt)) //has relevant sources
							{
								ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							if (authIntersectionSet.isEmpty())
							{
								authIntersectionSet.addAll(getAuthorities(authUnionSet));
							}
							else
							{
								authIntersectionSet.retainAll(getAuthorities(authUnionSet));
								//authIntersectionSet = getAuthorities(authIntersectionSet);
							}
							stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
						}
						for (HyperEdge outEdge: v.outEdges)
						{
							Map<StatementSource, List<String>> stmtSourceToAuthorities = new HashMap<StatementSource, List<String>> ();
							List<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
							for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
							{
								ArrayList<String> lstAuthorities =  getFedSumDMatchingSbjAuthorities(stmt, src);  //has authorities
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							if (authIntersectionSet.isEmpty())
							{
								authIntersectionSet.addAll(getAuthorities(authUnionSet));
							}
							else
							{
								authIntersectionSet.retainAll(getAuthorities(authUnionSet));
								//authIntersectionSet = getAuthorities(authIntersectionSet);
							}
							stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);

						}
						//System.out.println(v.label+":"+ authIntersectionSet);
						doSourcePrunning(stmtToLstAuthorities, authIntersectionSet);
					}
					//---------------------------------------------star node--------------------------------------
					else if( v.outEdges.size()>1) 
					{
						// System.out.println(v.label + " is star node");
						for(HyperEdge outEdge: v.outEdges) //has hyperedges or statement patterns
						{
							Map<StatementSource, List<String>> stmtSourceToAuthorities = new HashMap<StatementSource, List<String>> ();
							List<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
							for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
							{
								// long strt = System.currentTimeMillis();
								ArrayList<String> lstAuthorities =  getFedSumDMatchingSbjAuthorities(stmt, src);  //has authorities
								// System.out.println("loading sbj auth time : " + (System.currentTimeMillis()-strt));
								authUnionSet = getUnion(authUnionSet, lstAuthorities);

								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							if (authIntersectionSet.isEmpty())
							{
								authIntersectionSet.addAll(getAuthorities(authUnionSet));
							}
							else
							{
								authIntersectionSet.retainAll(getAuthorities(authUnionSet));
								//authIntersectionSet = getAuthorities(authIntersectionSet);
							}
							stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
							// System.out.println("InEdge: "+inEdge.subj+", "+inEdge.pred+", "+inEdge.obj+"\nInEdges Sources: "+stmtToSources.get( hyperEdgeToStmt.get(inEdge)));
						}
						// System.out.println(v.label+":"+ authIntersectionSet);
						doSourcePrunning(stmtToLstAuthorities,authIntersectionSet);
					}
					//----------------------------------------Path node ----------------------------------
					else if (v.outEdges.size() == 1 && v.inEdges.size() == 1)
					{
						//System.out.println(v.label + " is path node");
						for (HyperEdge inEdge: v.inEdges) //has hyperedges or statement patterns
						{
							Map<StatementSource, List<String>> stmtSourceToAuthorities = new HashMap<StatementSource, List<String>> ();
							List<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt =  hyperEdgeToStmt.get(inEdge);
							for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
							{
								ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							if (authIntersectionSet.isEmpty())
							{
								authIntersectionSet.addAll(getAuthorities(authUnionSet));
							}
							else
							{
								authIntersectionSet.retainAll(getAuthorities(authUnionSet));
								//authIntersectionSet = getAuthorities(authIntersectionSet);
							}
							stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
						}
						for (HyperEdge outEdge: v.outEdges)
						{
							Map<StatementSource, List<String>> stmtSourceToAuthorities = new HashMap<StatementSource, List<String>> ();
							List<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
							for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
							{
								ArrayList<String> lstAuthorities =  getFedSumDMatchingSbjAuthorities(stmt, src);  //has authorities
								//System.out.println(lstAuthorities);
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							
							if (authIntersectionSet.isEmpty())
							{
								authIntersectionSet.addAll(getAuthorities(authUnionSet));
							} else {
								authIntersectionSet.retainAll(getAuthorities(authUnionSet));
								//authIntersectionSet = getAuthorities(authIntersectionSet);
							}
							stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);

						}
						// System.out.println(v.label+":"+ authIntersectionSet);
						doSourcePrunning(stmtToLstAuthorities, authIntersectionSet);
					}
					//-------------------sink node ----------------------------------------------------

					else if( v.inEdges.size() > 1 && v.outEdges.isEmpty()) 
					{
						// System.out.println(v.label + " is sink node");
						//--- cant do source pruning for for literal value sink node 
						outerloop: 
							for (HyperEdge inEdge : v.inEdges) //has hyperedges or statement patterns
							{
								Map<StatementSource, List<String>> stmtSourceToAuthorities = new HashMap<StatementSource, List<String>> ();
								List<String> authUnionSet = new ArrayList<String>();
								StatementPattern stmt =  hyperEdgeToStmt.get(inEdge);
								for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
								{
									ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
									if (lstAuthorities.isEmpty())
										break outerloop;
									authUnionSet = getUnion(authUnionSet, lstAuthorities);
									stmtSourceToAuthorities.put(src, lstAuthorities);

								}
								if (authIntersectionSet.isEmpty())
								{
									authIntersectionSet.addAll(getAuthorities(authUnionSet));
									//System.out.println(authUnionSet);
								}
								else
								{
									authIntersectionSet.retainAll(getAuthorities(authUnionSet));
									//System.out.println(authUnionSet);
									//authIntersectionSet = getAuthorities(authIntersectionSet);
								}
								stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
							}
						// System.out.println(v.label+": sink "+ authIntersectionSet);
						doSourcePrunning(stmtToLstAuthorities, authIntersectionSet);
					}


				}
			}
		}
		
		int newSources = 0;
		for (StatementPattern stmt: stmtToSources.keySet())
			newSources = newSources + stmtToSources.get(stmt).size();
		if (log.isDebugEnabled()) {
			log.debug("Total Triple pattern-wise sources selected : "+ newSources);
		}

		return stmtToSources;
	}
	
	/**
	 * Get he set of URI authorities from the given set of URIs
	 * @param uriSet URI set
	 * @return set of distinct authorities
	 */
	private List<String> getAuthorities(List<String> uriSet) {
		Set<String> authorities = new HashSet<String>();
		//System.out.println(uriSet);
		for(String uri:uriSet)
		{
			String[] sbjPrts = uri.split("/");
			authorities.add(sbjPrts[0]+"//"+sbjPrts[2]);
		}
		//System.out.println(authorities);
		return Arrays.asList(authorities.toArray(new String[]{}));
	}

	/**
	 *  Union of two Sets
	 * @param authUnionSet First Set
	 * @param lstAuthorities Second Set
	 * @return Union of two sets
	 */
	public static List<String> getUnion(List<String> authUnionSet, List<String> lstAuthorities)
	{
		for (String authority : lstAuthorities)
			if (!authUnionSet.contains(authority))
				authUnionSet.add(authority);
		return authUnionSet;
	}
	
	/**
	 * Remove irrelvant sources from each triple pattern according to step-2 of our source selection
	 * @param stmtToLstAuthorities A map which stores the list of authorities for each capable source of a triple pattern
	 * @param authIntersectionSet The common authorities set. see step 2 at FedSum paper for the usage of this list
	 */
	private void doSourcePrunning(Map<StatementPattern, Map<StatementSource, List<String>>> stmtToLstAuthorities, List<String> authIntersectionSet) 
	{
		//System.out.println( stmtToLstAuthorities);
		Boolean finalPrunningRequired = false;   // do we need a third step of prunning. i.e., using after uri authority parts?  if the number of relevant sources for a stmt is greater than 1 then we may further prune it
		Map<StatementPattern, Map<StatementSource, List<String>>> newStmtToLstAuthorities = new ConcurrentHashMap <StatementPattern, Map<StatementSource, List<String>>> ();
		//ArrayList<String> onlyAuth = new ArrayList<String> () ;  //list containing those uri's which are exactly an authority. we have to extend these authorities into uris with all possible extensions
		List<String> allNewURIs = new ArrayList<String> () ;
		for (StatementPattern stmt:stmtToLstAuthorities.keySet())
		{
			Map<StatementSource, List<String>> newStmtSourceToLstAuthorities = new HashMap<StatementSource, List<String>>();
			// System.out.println("\n\n");
			// System.out.println("Intersection set: "+authIntersectionSet);
			Map<StatementSource, List<String>> stmtSourceToLstAuthorities = stmtToLstAuthorities.get(stmt);
			for(StatementSource src:stmtSourceToLstAuthorities.keySet())
			{
				// System.out.println("\n");
				List<String> srcAuthSet = new ArrayList<String>();
				srcAuthSet.addAll(getAuthorities(stmtSourceToLstAuthorities.get(src)));
				srcAuthSet.retainAll(authIntersectionSet);

				if (!srcAuthSet.isEmpty())  //consider the sources and statements for the second iteration of source prunning. this time including outside part of uri authority
				{
					//System.out.println("\n"+src.getEndpointID()+ ": "+srcAuthSet+": "+ stmt.getPredicateVar().getSignature() );
					List<String> remURIs = stmtSourceToLstAuthorities.get(src);
					List<String> newURIs = new ArrayList<String>();
					for (String uri:authIntersectionSet)
					{
						for (String remURI:remURIs)
						{
							if (remURI.contains(uri))
								newURIs.add(remURI);
						}
					}
					//System.out.println("Actual: "+ newURIs);

					allNewURIs = getUnion(allNewURIs, newURIs);
					//onlyAuth = getUnion(onlyAuth,getOnlyAuthorities(newURIs));
					newStmtSourceToLstAuthorities.put(src, newURIs);
				}

				//------------remove irrelevant source
				if (srcAuthSet.isEmpty())
				{
					List<StatementSource> sources = stmtToSources.get(stmt);
					synchronized (sources) {
						sources.remove(src);
						if(sources.size()>1)
							finalPrunningRequired = true; 
					}
				}
			}

			newStmtToLstAuthorities.put(stmt, newStmtSourceToLstAuthorities);	
		}
		// System.out.println("\nOnly authority URIs: "+onlyAuth);

		//Map<String, ArrayList<String>> allCombURIs =  getAllCombinationsOfOnlyAuth(onlyAuth,allNewURIs);
		//System.out.println("\n All combination: "+allCombURIs);
		// if(allCombURIs.size()>0)
		//System.out.println("\n\n Before adding combinations: "+ newStmtToLstAuthorities);
		Map<StatementPattern, Map<StatementSource, List<String>>> finalStmtToLstAuthorities = addAllCombinations(newStmtToLstAuthorities, allNewURIs);
		// System.out.println("\n\n After adding combinations: "+ finalStmtToLstAuthorities);

		if(finalPrunningRequired==true)
			doFinalPruning(finalStmtToLstAuthorities);
	}
	/**
	 * Add all possible combinations to sub-prefixes. Sub-prefixes are like most common automata's
	 * @param newStmtToLstAuthorities Statement Patter to list of authorities map
	 * @param allNewURIs Set of prefixes which needs to be checked for adding all possibilities
	 * @return Hash map of statement pattern to sources after adding combinations 
	 */

	private Map<StatementPattern, Map<StatementSource, List<String>>> addAllCombinations(
			Map<StatementPattern, Map<StatementSource, List<String>>> newStmtToLstAuthorities, List<String> allNewURIs) 
	{
		Map<StatementPattern, Map<StatementSource, List<String>>> hm = new HashMap<StatementPattern, Map<StatementSource, List<String>>>();
		TrieNode tn = Trie.constructTrie(allNewURIs, 100);
		
		for(StatementPattern stmt:newStmtToLstAuthorities.keySet())
		{
			Map<StatementSource, List<String>> newStmtSourceToLstAuthorities = new HashMap<StatementSource, List<String>>();
			Map<StatementSource, List<String>> stmtSourceToLstAuthorities = newStmtToLstAuthorities.get(stmt);
			for(StatementSource src:stmtSourceToLstAuthorities.keySet())
			{
				List<String> srcURISet = stmtSourceToLstAuthorities.get(src);
				List<String> newSrcURISet = new ArrayList<String> ();
				for(String uri:srcURISet)
				{
					newSrcURISet = getUnion(newSrcURISet, Trie.getAllCombinations(uri, tn));
				}
				newStmtSourceToLstAuthorities.put(src, newSrcURISet);
			}
			hm.put(stmt, newStmtSourceToLstAuthorities);
		}
		return hm;
	}
	
	/**
	 *  Do the third phase of pruning. 1) using index and sparql ask 2) using authorities 3) using prefixes
	 * @param newStmtToLstAuthorities A map of statment pattern to list authorities
	 */
	private void doFinalPruning(Map<StatementPattern, Map<StatementSource, List<String>>> newStmtToLstAuthorities)
	{
		// System.out.println("\nAfter : "+ newStmtToLstAuthorities);

		List<String> intersectionSet = getIntersection(newStmtToLstAuthorities);

		//System.out.println("Final Intersection Set: " +intersectionSet);
		for(StatementPattern stmt:newStmtToLstAuthorities.keySet())
		{
			Map<StatementSource, List<String>> stmtSourceToLstAuthorities = newStmtToLstAuthorities.get(stmt);
			for(StatementSource src:stmtSourceToLstAuthorities.keySet())
			{
				List<String> srcURISet = stmtSourceToLstAuthorities.get(src);
				srcURISet.retainAll(intersectionSet) ;
				//------------remove irrelevant source
				if (srcURISet.isEmpty())
				{
					List<StatementSource> sources = stmtToSources.get(stmt);
					synchronized (sources) {
						sources.remove(src);
					}
				}
			}
		}

	}
	
	/**
	 *  Get interstion of the list of prefixes
	 * @param newStmtToLstAuthorities statment pattern to list of authorities
	 * @return Intersection set
	 */
	private List<String> getIntersection(Map<StatementPattern, Map<StatementSource, List<String>>> newStmtToLstAuthorities)
	{
		List<String> intersectionSet = new ArrayList<String>();
		for(StatementPattern stmt:newStmtToLstAuthorities.keySet())
		{
			Map<StatementSource, List<String>> stmtSourceToLstAuthorities = newStmtToLstAuthorities.get(stmt);
			List<String> stmtURIs = new ArrayList<String> ();  //get all URIs (over all relevant sources) of a stmt
			for(StatementSource src:stmtSourceToLstAuthorities.keySet())
			{
				List<String> srcURISet = stmtSourceToLstAuthorities.get(src);
				stmtURIs = getUnion(stmtURIs, srcURISet);
			}
			if (intersectionSet.isEmpty())
				intersectionSet = stmtURIs;
			else
				intersectionSet.retainAll(stmtURIs);
			//authIntersectionSet = getAuthorities(authIntersectionSet);

		}
		return intersectionSet;
	}

	/**
	 *  Get matching Subject authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @return List of authorities
	 */

	public ArrayList<String> getFedSumDMatchingSbjAuthorities(StatementPattern stmt, StatementSource src)
	{
		String endPointUrl = "http://" + src.getEndpointID().replace("sparql_", "");
		endPointUrl = endPointUrl.replace("_", "/");
		ArrayList<String> sbjAuthorities = new ArrayList<String>();

		String  queryString = getFedSumSbjAuthLookupQuery(stmt, endPointUrl) ;

		TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext())
			sbjAuthorities.add(result.next().getValue("sbjAuth").stringValue());

		return sbjAuthorities;
	}
	/**
	 *  A SPARQL query to retrieve matching subject authorities for a capable source of a triple pattern
	 * @param stmt Triple Pattern
	 * @param endPointUrl Url of the data source
	 * @return SPARQL query
	 */
	public String getFedSumSbjAuthLookupQuery(StatementPattern stmt,String endPointUrl)
	{
		String queryString = null;
		String s = null, p = null, o = null, sa = null, oa = null;
		if (stmt.getSubjectVar().getValue()!=null)
		{
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa = sbjPrts[0]+"//"+sbjPrts[2];
		}  

		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		}

		if (stmt.getObjectVar().getValue() != null)
		{
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if (objPrts.length > 2) {     //add only URI
				oa = objPrts[0] + "//" + objPrts[2];
			}
		}
		
		//------------------------------------------------
		if (p != null) // if predicate is bound
		{
			queryString = "Prefix ds:<http://aksw.org/quetsal/> "
					+ "SELECT  DISTINCT ?sbjAuth "
					+ " WHERE {?s ds:url <" + endPointUrl+">. "
					+ " 		?s ds:capability ?cap."
					+ "        ?cap ds:predicate <"+p+">."
					+ "?cap ds:sbjPrefix  ?sbjAuth. }" ;	
		}
		else //predicate is not bound
		{
			if (sa == null && oa == null)  //and subject , object are not bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?sbjAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:sbjPrefix  ?sbjAuth. }" ;
			}
			else if (sa != null && oa != null) //and subject, object are bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?sbjAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:sbjPrefix  ?sbjAuth. "
						+ "?cap ds:objPrefix <"+oa+">. "
						+ "FILTER REGEX (str(?sbjAuth),'"+sa+"')"
						+ "}" ;
			}
			else if (sa != null && oa == null) //and subject only is bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?sbjAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:sbjPrefix  ?sbjAuth. "
						+ "FILTER REGEX (str(?sbjAuth),'"+sa+"')"
						+ "}" ;
			}
			else if (sa == null && oa != null) //and object is  bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?sbjAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:sbjPrefix  ?sbjAuth. "
						+ "?cap ds:objPrefix <"+oa+">. "
						+ "}" ;
			}

		}

		return queryString;
	}
	
	/**
	 *  Get matching object predicate authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @param v Vertex
	 * @return List of authorities
	 * @throws RepositoryException  Repository Error
	 * @throws MalformedQueryException Memory Error
	 * @throws QueryEvaluationException Execution Error
	 */
	public ArrayList<String> FedSumD_getMatchingObjAuthorities(StatementPattern stmt, StatementSource src, Vertex v) throws RepositoryException, MalformedQueryException, QueryEvaluationException 
	{
		String endPointUrl = "http://"+src.getEndpointID().replace("sparql_", "");
		endPointUrl = endPointUrl.replace("_", "/");
		String p = null;
		ArrayList<String> objAuthorities = new ArrayList<String>();
		if (stmt.getPredicateVar().getValue() != null)
			p = stmt.getPredicateVar().getValue().stringValue();
		else
			p = stmt.getPredicateVar().getName().toString(); 
		String  queryString = getFedSumObjAuthLookupQuery(stmt, endPointUrl,v) ;
		TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		if (p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) //for rdf:type

		{
			while(result.hasNext())
			{
				String o = result.next().getValue("objAuth").stringValue();
				String[] objPrts = o.split("/");
				objAuthorities.add(objPrts[0]+"//"+objPrts[2]);
			}

		}
		//		 else if(p.equals(v.label)) // node is a predicate of a triple pattern. enable this code if you are interested in looking for predicate join triples. 
		//		 {
		//			 while(result.hasNext())
		//			   {
		//				    String o = result.next().getValue("predAuth").stringValue();
		//				 	String[] objPrts = o.split("/");
		//				 	 objAuthorities.add(objPrts[0]+"//"+objPrts[2]);
		//			   } 
		//		 }
		else
		{
			while(result.hasNext())
			{
				objAuthorities.add(result.next().getValue("objAuth").stringValue());

			}
		}
		return objAuthorities;
	}
	/**
	 *  A SPARQL query to retrieve matching object authorities for a capable source of a triple pattern
	 * @param stmt Triple Pattern
	 * @param endPointUrl Url of the data source
	 * @param v Vertex
	 * @return SPARQL query
	 */
	public String getFedSumObjAuthLookupQuery(StatementPattern stmt, String endPointUrl, Vertex v)
	{
		String queryString = null;
		String s = null, p = null, o = null, sa = null, oa = null;
		if (stmt.getSubjectVar().getValue() != null)
		{
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa =sbjPrts[0] + "//" + sbjPrts[2];
		}

		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		} else if (stmt.getPredicateVar().getName().equals(v.label)) {
			p = stmt.getPredicateVar().getName();
		}

		if (stmt.getObjectVar().getValue() != null)
		{
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if (objPrts.length > 1) {     //add only URI
				oa = objPrts[0]+"//"+objPrts[2];
			}
		}

		//---------------------------You can enable this code for more advance join ware source selection---------------------
		//	if(v.label.equals(p))   // if vertex is predicate of a triple pattern 
		//	{
		//		if(p.startsWith("http://") || p.startsWith("ftp://")) // if predicate is bound. Note this needs to be properly validated using UrlValidator class may be. 
		//		{
		//		 queryString = "Prefix ds:<http://aksw.org/quetsal/> "
		//			   		+ "SELECT DISTINCT  ?predAuth "
		//				   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
		//					+ " 		?s ds:capability ?cap."
		//					+ "        ?cap ds:predicate ?predAuth."
		//					+ "FILTER REGEX (str(?predAuth), '"+p+"')"
		//				  		     + "}" ;	
		//		}
		//		else //predicate is not bound
		//		{
		//			if(sa == null && oa == null)  //and subject , object are not bound
		//			{
		//				 queryString = "Prefix ds:<http://aksw.org/quetsal/> "
		//					   		+ "SELECT  Distinct ?predAuth "
		//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
		//							+ " 		?s ds:capability ?cap."
		//							         + "?cap ds:predicate ?predAuth. }" ;
		//			}
		//			else if(sa != null && oa != null) //and subject, object are bound
		//			{
		//				 queryString = "Prefix ds:<http://aksw.org/quetsal/> "
		//					   		+ "SELECT  Distinct ?predAuth "
		//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
		//							+ " 		?s ds:capability ?cap."
		//							         + "?cap ds:predicate ?predAuth. "
		//							         + "?cap ds:objPrefix <"+oa+">. "
		//							         + "?cap ds:sbjPrefix <"+sa+">. "
		//					           + "}" ;
		//			}
		//			else if(sa == null && oa != null) //and object only is bound
		//			{
		//				 queryString = "Prefix ds:<http://aksw.org/quetsal/> "
		//					   		+ "SELECT  Distinct ?predAuth "
		//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
		//							+ " 		?s ds:capability ?cap."
		//							         + "?cap ds:predicate ?predAuth. "
		//							         + "?cap ds:objPrefix <"+oa+">. "
		//					           + "}" ;
		//			}
		//			else if(sa != null && oa == null) //and subject is  bound
		//			{
		//				 queryString = "Prefix ds:<http://aksw.org/quetsal/> "
		//					   		+ "SELECT  Distinct ?predAuth "
		//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
		//							+ " 		?s ds:capability ?cap."
		//							         + "?cap ds:predicate ?predAuth.  "
		//							         + "?cap ds:sbjPrefix <"+sa+">. "
		//							         + "}" ;
		//			}
		//		}
		//	}
		//	//-------------------
		//	else
		//	{
		if(p != null && !stmt.getPredicateVar().getName().equals(v.label)) // if predicate is bound
		{
			queryString = "Prefix ds:<http://aksw.org/quetsal/> "
					+ "SELECT DISTINCT  ?objAuth "
					+ " WHERE {?s ds:url <" + endPointUrl+">. "
					+ " 		?s ds:capability ?cap."
					+ "        ?cap ds:predicate <"+p+">."
					+ "?cap ds:objPrefix  ?objAuth. }" ;	
		}
		else //predicate is not bound
		{
			if (sa == null && oa == null)  //and subject , object are not bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?objAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:objPrefix  ?objAuth. }" ;
			}
			else if (sa != null && oa != null) //and subject, object are bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?objAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:objPrefix  ?objAuth. "
						+ "?cap ds:objPrefix <"+oa+">. "
						+ "FILTER REGEX (str(?objAuth),'"+oa+"')"
						+ "}" ;
			}
			else if (sa == null && oa != null) //and object only is bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?objAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:objPrefix  ?objAuth. "
						+ "FILTER REGEX (str(?objAuth),'"+oa+"')"
						+ "}" ;
			}
			else if (sa != null && oa == null) //and subject is  bound
			{
				queryString = "Prefix ds:<http://aksw.org/quetsal/> "
						+ "SELECT  Distinct ?objAuth "
						+ " WHERE {?s ds:url <" + endPointUrl+">. "
						+ " 		?s ds:capability ?cap."
						+ "?cap ds:objPrefix  ?objAuth. "
						+ "?cap ds:sbjPrefix <"+sa+">. "
						+ "}" ;
			}

		}


		return queryString;
	}

	protected static class SourceSelectionExecutorWithLatch {

		/**
		 * /**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch.
		 * Results are added to the map of the source selection instance. Errors 
		 * are reported as {@link OptimizationException} instances.
		 * @param hibiscusSourceSelection Quetsal Source Selection
		 * @param tasks Set of SPARQL ASK tasks
		 * @param cache Cache
		 */
		public static void run(TBSSSourceSelectionOriginal hibiscusSourceSelection, List<CheckTaskPair> tasks, Cache cache) {
			new SourceSelectionExecutorWithLatch(hibiscusSourceSelection).executeRemoteSourceSelection(tasks, cache);
		}		

		private final TBSSSourceSelectionOriginal sourceSelection;
		private ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();

		private SourceSelectionExecutorWithLatch(TBSSSourceSelectionOriginal hibiscusSourceSelection) {
			this.sourceSelection = hibiscusSourceSelection;
		}

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch
		 * 
		 * @param tasks
		 */
		private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
			if (tasks.isEmpty())
				return;

			//initiatorThread = Thread.currentThread();
			//latch = new CountDownLatch(tasks.size());
			List<Exception> errors = new ArrayList<Exception>();
			List<Future<Void>> futures = new ArrayList<Future<Void>>();
			for (CheckTaskPair task : tasks)
				futures.add(scheduler.schedule(new ParallelCheckTask(task.e, task.t, sourceSelection), QueryInfo.getPriority() + 1));

			for (Future<Void> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					log.debug("Error during source selection. Thread got interrupted.");
					break;
				} catch (Exception e) {
					errors.add(e);
				}	
			}

			// check for errors:
			if (!errors.isEmpty()) {
				log.error(errors.size() + " errors were reported:");
				for (Exception e : errors)
					log.error(ExceptionUtil.getExceptionString("Error occured", e));

				Exception ex = errors.get(0);
				errors.clear();
				if (ex instanceof OptimizationException)
					throw (OptimizationException)ex;

				throw new OptimizationException(ex.getMessage(), ex);
			}
		}
	}


	public class CheckTaskPair {
		public final Endpoint e;
		public final StatementPattern t;
		public CheckTaskPair(Endpoint e, StatementPattern t){
			this.e = e;
			this.t = t;
		}		
	}


	/**
	 * Task for sending an ASK request to the endpoints (for source selection)
	 * 
	 * @author Andreas Schwarte
	 */
	protected static class ParallelCheckTask implements Callable<Void> {

		final Endpoint endpoint;
		final StatementPattern stmt;
		final TBSSSourceSelectionOriginal sourceSelection;

		public ParallelCheckTask(Endpoint endpoint, StatementPattern stmt, TBSSSourceSelectionOriginal sourceSelection) {
			this.endpoint = endpoint;
			this.stmt = stmt;
			this.sourceSelection = sourceSelection;
		}


		@Override
		public Void call() throws Exception {
			try {
				TripleSource t = endpoint.getTripleSource();
				RepositoryConnection conn = endpoint.getConn(); 

				boolean hasResults = t.hasStatements(stmt, conn, EmptyBindingSet.getInstance());

				CacheEntry entry = CacheUtils.createCacheEntry(endpoint, hasResults);
				sourceSelection.cache.updateEntry( new SubQuery(stmt), entry);

				if (hasResults)
					sourceSelection.addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));

				return null;
			} catch (Exception e) {
				throw new OptimizationException("Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
			}
		}		
	}
}