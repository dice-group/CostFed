
package org.aksw.simba.quetsal.core;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.simba.quetsal.datastructues.HyperGraph.HyperEdge;
import org.aksw.simba.quetsal.datastructues.HyperGraph.Vertex;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.EndpointManager;
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
import com.fluidops.fedx.evaluation.concurrent.ParallelExecutor;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.optimizer.SourceSelection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.SubQuery;
import com.fluidops.fedx.util.QueryStringUtil;

import info.aduna.iteration.CloseableIteration;
/**
 * Perform triple pattern-wise source selection using FedSumaries, cache or SPARQL ASK. 
 * Note that this is first phase of our source selection. In second phase we perform source filtering for each of the statement pattern using Hyper Graphs 
 * @author Saleem
 *
 */
public class HibiscusSourceSelection extends SourceSelection {
	static Logger log = Logger.getLogger(HibiscusSourceSelection.class);
	
	public Map<HyperEdge,StatementPattern> hyperEdgeToStmt = new HashMap<HyperEdge,StatementPattern>(); //Hyper edges to Triple pattern Map
	public List<Set<Vertex>> theDNFHyperVertices = new ArrayList<Set<Vertex>>(); // Sets of vertices in different DNF hypergraphs
	
	
	///protected final List<StatementPattern> triplePatterns;
	
	public HibiscusSourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		super(endpoints, cache, queryInfo);
	}
	
	public List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();

	/**
	 * Perform triple pattern-wise source selection for the provided statement patterns of a SPARQL query using HiBISCuS Summaries, cache or remote ASK queries.
	 *Remote ASK queries are evaluated in parallel using the concurrency infrastructure of FedX. Note,
	 * that this method is blocking until every source is resolved.
	 * Recent SPARQL ASK operations are cached for future use.
	 * The statement patterns are replaced by appropriate annotations in this process.
	 * Hypergraphs are created in step 1 of source selection and are used in step 2 i.e. prunning of 
	 * triple pattern-wise selected sources. 
	 * @param DNFgrps Set of BGP's in SPARQL Query
	 * @return stmtToSources Map of triple patterns to relevant sources
	 */
	@Override
	public void performSourceSelection(List<List<StatementPattern>> bgpGroups) 
	{
		long tp = 0 ;
		for(List<StatementPattern> stmts : bgpGroups)  //DNFgrp => bgp
		{
			 Set<Vertex> V = new HashSet<Vertex>();   //--Set of all vertices used in our hypergraph. each subject, predicate and object of a triple pattern is one node until it is repeated
		 //    HashSet<HyperEdge> E = new HashSet<HyperEdge>();  //-- set of all hyperedges. an edge contains subject, predicate and object node of a triple pattern

		// for each statement determine the relevant sources. Note each  statement pattern is a hyperedge as depicted in step 1 source selection algo given in FedSum paper
		for (StatementPattern stmt : stmts) 
		{
			 tp++;
			//cache.clear();
			stmtToSources.put(stmt, new ArrayList<StatementSource>());
			//--------
				String s,p,o, sa = "null", oa="null", sbjVertexLabel, objVertexLabel, predVertexLabel;
				Vertex sbjVertex, predVertex,objVertex ;
				if (stmt.getSubjectVar().getValue()!=null)
				{
					s = stmt.getSubjectVar().getValue().stringValue();
					String[] sbjPrts = s.split("/");
					sa =sbjPrts[0]+"//"+sbjPrts[2];
					//-- add subjectVertex
					sbjVertexLabel = s;
					sbjVertex = new Vertex(stmt.getSubjectVar(), sbjVertexLabel);
					if (!vertexExist(sbjVertex,V))
						V.add(sbjVertex);
				}
				else
				{
					s ="null";  
					//-- add subjectVertex
					sbjVertexLabel=stmt.getSubjectVar().getName();
					sbjVertex= new Vertex(stmt.getSubjectVar(), sbjVertexLabel);
					if(!vertexExist(sbjVertex,V))
					V.add(sbjVertex);
				}
				if (stmt.getPredicateVar().getValue()!=null)
				{
					p = stmt.getPredicateVar().getValue().stringValue();
					//-- add predicateVertex
					predVertexLabel=p;
					predVertex = new Vertex(stmt.getPredicateVar(), predVertexLabel);
					if(!vertexExist(predVertex,V))
					V.add(predVertex);
				}
				else
				{
					p ="null"; 
					//-- add predicateVertex
					predVertexLabel=stmt.getPredicateVar().getName();
					predVertex= new Vertex(stmt.getPredicateVar(), predVertexLabel);
					if(!vertexExist(predVertex,V))
					V.add(predVertex);
				}
				if (stmt.getObjectVar().getValue()!=null)
				{
					o = stmt.getObjectVar().getValue().stringValue();
					String[] objPrts = o.split("/");
					if ((objPrts.length>1))      //add only URI
						oa =objPrts[0]+"//"+objPrts[2];
			         else
			        	 oa = "null";
					//-- add predicateVertex
					objVertexLabel=o;
					objVertex = new Vertex(stmt.getObjectVar(), objVertexLabel);
					if(!vertexExist(objVertex,V))
						V.add(objVertex);
				}
				else
				{
					o = "null"; 
					//-- add predicateVertex
					objVertexLabel=stmt.getObjectVar().getName();
					objVertex= new Vertex(stmt.getObjectVar(), objVertexLabel);
					if(!vertexExist(objVertex,V))
					V.add(objVertex);
				}
				 //-------Step 1 of our source selection---i.e Triple pattern-wise source selection----
			if(QuetzalConfig.mode.equals("ASK_dominant"))   //---ASK_dominant algo
			{
	          if(s.equals("null") && p.equals("null")&& o.equals("null"))
	          {
	        	  for (Endpoint e : endpoints) 
	        		  addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
	          }
	          else if (!p.equals("null"))
	          {
	        	  if(p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && !o.equals("null"))  
	        		  FedSumClassLookup(stmt,p,o);
	        	  else if (!QuetzalConfig.commonPredicates.contains(p) || (s.equals("null") && o.equals("null")))
	        		  FedSumLookup(stmt,sa,p,oa);
	        	  else
	        		  cache_ASKselection(stmt);    	 
	           }
	          
	          else
	        	     cache_ASKselection(stmt);
			}
			else // Index_dominant source selection algo
			{
				 if(s.equals("null") && p.equals("null")&& o.equals("null"))
		          {
		        	  for (Endpoint e : endpoints) 
		        		  addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
		          }
				 else if (!s.equals("null") || !p.equals("null"))
				 {
					 if(!p.equals("null") && p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && !o.equals("null"))
						 FedSumClassLookup(stmt,p,o);
					 else
						 FedSumLookup(stmt,sa,p,oa);
				 }
				 else
					 cache_ASKselection(stmt);
			}
			//--------add hyperedges
			 HyperEdge hEdge = new HyperEdge(sbjVertex,predVertex,objVertex);
			// E.add(hEdge);
			 if(!(getVertex(sbjVertexLabel,V)==null))
				 sbjVertex = getVertex(sbjVertexLabel,V);
			 if(!(getVertex(predVertexLabel,V)==null))
				 predVertex = getVertex(predVertexLabel,V);
			 if(!(getVertex(objVertexLabel,V)==null))
				 objVertex = getVertex(objVertexLabel,V);
			 sbjVertex.outEdges.add(hEdge); predVertex.inEdges.add(hEdge); objVertex.inEdges.add(hEdge);
			 hyperEdgeToStmt.put(hEdge, stmt);
		
		
					
		}
		theDNFHyperVertices.add(V);
		}
		// long askStartTime = System.currentTimeMillis();

		// if remote checks are necessary, execute them using the concurrency
		// infrastructure and block until everything is resolved
		if (remoteCheckTasks.size()>0) {
			
			SourceSelectionExecutorWithLatch.run(this, remoteCheckTasks, cache);
			System.out.println("Number of ASK request: " + remoteCheckTasks.size());
		}
		else
			System.out.println("Number of ASK request: 0");
		// long endTime = System.currentTimeMillis();
		//System.out.println("Ask queries Cost: Execution time(msec) : "+ (endTime-askStartTime));
	     int triplePatternWiseSources = 0 ;
		for (StatementPattern stmt : stmtToSources.keySet()) {
			
			List<StatementSource> sources = stmtToSources.get(stmt);
			//System.out.println("-----------\n"+stmt);
			//System.out.println(sources);
			triplePatternWiseSources = triplePatternWiseSources + sources.size();
			// if more than one source -> StatementSourcePattern
			// exactly one source -> OwnedStatementSourcePattern
			// otherwise: No resource seems to provide results
			
			if (sources.size()>1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
				for (StatementSource s : sources)
					stmtNode.addStatementSource(s);
				stmt.replaceWith(stmtNode);
				
			}
		
			else if (sources.size()==1) {
				stmt.replaceWith( new ExclusiveStatement(stmt, sources.get(0), queryInfo));
			}
			
			else {
				if (log.isDebugEnabled())
					log.debug("Statement " + QueryStringUtil.toString(stmt) + " does not produce any results at the provided sources, replacing node with EmptyStatementPattern." );
				stmt.replaceWith( new EmptyStatementPattern(stmt));
			}
		}
		 
		//System.out.println("Total Triple Pattern-wise selected sources in step 1 of FedSum Source selection: " + triplePatternWiseSources);
		//System.out.println("step 1 time: "+ (System.currentTimeMillis()-step1Strt));
		 //--------------------------------Step 2 of our source selection i.e. pruning selected sources in step 1 using hypergraphs--------------
//		for(int DNFkey:DNFHyperVertices.keySet())
//		{
//			HashSet<Vertex> V = DNFHyperVertices.get(DNFkey);
//			System.out.println("--------------new DNF Graph---------");
//		 for (Vertex v:V)
//		  System.out.println("vertex: "+v+" , InEdges: "+v.inEdges.size() + " outEdges: "+v.outEdges.size());
//		}
		if (triplePatternWiseSources>tp)
		stmtToSources =  pruneSources(theDNFHyperVertices);
		}

	/**
	 * Retrieve a vertex having a specific label from a set of Vertrices
	 * @param label Label of vertex to be retrieved
	 * @param V Set of vertices
	 * @return Vertex if exist otherwise null
	 */
	public Vertex getVertex(String label, Set<Vertex> V) {
		for(Vertex v:V)
		{
			if(v.label.equals(label))
				return v;
		}
		return null;
	}
	/**
	 * Check if a  vertex already exists in set of all vertices
	 * @param sbjVertex Subject Vertex
	 * @param V Set of all vertices
	 * @return value Boolean value
	 */
public  boolean vertexExist(Vertex sbjVertex, Set<Vertex> V) {
		for(Vertex v:V)
		{
			if(sbjVertex.label.equals(v.label))
				return true;
		}
		return false;
	}
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
			if (a==StatementSourceAssurance.HAS_LOCAL_STATEMENTS) 
			addSource(stmt, new StatementSource(e.getId(), StatementSourceType.LOCAL));
			else if (a==StatementSourceAssurance.HAS_REMOTE_STATEMENTS) 
				addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
			else if (a==StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) 
		    	remoteCheckTasks.add( new CheckTaskPair(e, stmt));
	   }
}

	/**
	 * Search HiBISCuS index for the given triple pattern p with sbj authority and obj authority.
	 * Note: sa, oa can be null i.e. for unbound tuple 
	 * @param stmt Statement pattern	
	 * @param sa Subject authority
	 * @param p Predicate
	 * @param oa Object authority
	 * @throws QueryEvaluationException Query Error
	 * @throws MalformedQueryException  Memory Error
	 * @throws RepositoryException  Repository Erro
	 */
	public void FedSumLookup(StatementPattern stmt, String sa, String p, String oa) throws QueryEvaluationException, RepositoryException, MalformedQueryException {
		    String  queryString = getFedSumLookupQuery(sa,p,oa) ;
		    TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			 TupleQueryResult result = tupleQuery.evaluate();
			   while(result.hasNext())
			   {
				  String endpoint = result.next().getValue("url").stringValue();
					String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
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
	if(!p.equals("null"))	
	{
		if(sa.equals("null") && oa.equals("null"))
		{
		          queryString = "Prefix ds:<http://aksw.org/fedsum/> "
			   		+ "SELECT  Distinct ?url "
				   	+ " WHERE {?s ds:url ?url. "
					+ " 		?s ds:capability ?cap. "
					+ "		   ?cap ds:predicate <" + p + ">.}"
		
					 ;
		}
		else if (!sa.equals("null") && !oa.equals("null"))
		{
			 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
				   		+ "SELECT  Distinct ?url "
					   	+ " WHERE {?s ds:url ?url. "
						+ " 		?s ds:capability ?cap. "
						+ "		   ?cap ds:predicate <" + p + ">."
								+ "?cap ds:sbjAuthority  <" +sa + ">."
								+ "?cap ds:objAuthority  <" + oa + ">}" ;	
		}
		else if (!sa.equals("null") && oa.equals("null"))
		{
			 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
				   		+ "SELECT  Distinct ?url "
					   	+ " WHERE {?s ds:url ?url. "
						+ " 		?s ds:capability ?cap. "
						+ "		   ?cap ds:predicate <" + p + ">."
								+ "?cap ds:sbjAuthority  <" +sa + ">. }"
								 ;	
		}
		else if (sa.equals("null") && !oa.equals("null"))
		{
			 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
				   		+ "SELECT  Distinct ?url "
					   	+ " WHERE {?s ds:url ?url. "
						+ " 		?s ds:capability ?cap. "
						+ "		   ?cap ds:predicate <" + p + ">."
						+ "?cap ds:objAuthority  <" + oa + ">}" ;	
		}
	}
	else
	{
		if (!sa.equals("null") && !oa.equals("null"))
		{
			 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
				   		+ "SELECT  Distinct ?url "
					   	+ " WHERE {?s ds:url ?url. "
						+ " 		?s ds:capability ?cap. "
						+ "?cap ds:sbjAuthority  <" +sa + ">."
						+ "?cap ds:objAuthority  <" + oa + ">}" ;	
		}
		else if (!sa.equals("null") && oa.equals("null"))
		{
			 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
				   		+ "SELECT  Distinct ?url "
					   	+ " WHERE {?s ds:url ?url. "
						+ " 		?s ds:capability ?cap. "
						+ "?cap ds:sbjAuthority  <" +sa + ">. }"
								 ;	
		}
		else if (sa.equals("null") && !oa.equals("null"))
		{
			 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
				   		+ "SELECT  Distinct ?url "
					   	+ " WHERE {?s ds:url ?url. "
						+ " 		?s ds:capability ?cap. "
						+ "?cap ds:objAuthority  <" + oa + ">}" ;	
		}
	}
		
		return queryString;
	}
	/**
	 * HiBISCuS Index lookup for rdf:type and its its corresponding values
	 * @param p Predicate i.e. rdf:type
	 * @param o Predicate value
	 * @param stmt Statement Pattern
	 * @throws RepositoryException Repository Error
	 * @throws MalformedQueryException Query Error
	 * @throws QueryEvaluationException Query Execution Error
	 */
	public void FedSumClassLookup(StatementPattern stmt, String p, String o) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		
		  String  queryString = "Prefix ds:<http://aksw.org/fedsum/> "
			   		+ "SELECT  Distinct ?url "
				   	+ " WHERE {?s ds:url ?url. "
					+ " 		?s ds:capability ?cap. "
					+ "		   ?cap ds:predicate <" + p + ">."
							+ "?cap ds:objAuthority  <" + o + "> }" ;
		    TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			 TupleQueryResult result = tupleQuery.evaluate();
			   while(result.hasNext())
			   {
				  String endpoint = result.next().getValue("url").stringValue();
					String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
					addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
			   }
	}
	//------------------------------------------------------------
	/**
	 * Retrieve a set of relevant sources for this query.
	 * @return endpoints set of relevant sources
	 */
	public Set<Endpoint> getRelevantSources() {
		Set<Endpoint> endpoints = new HashSet<Endpoint>();
		for (List<StatementSource> sourceList : stmtToSources.values())
			for (StatementSource source : sourceList)
				endpoints.add( EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID()));
		return endpoints;
	}	
	
	/**
	 * Add a source to the given statement in the map (synchronized through map)
	 * 
	 * @param stmt Triple Pattern
	 * @param source Source or SPARQL endpoint
	 */
	public void addSource(StatementPattern stmt, StatementSource source) {
		// The list for the stmt mapping is already initialized
		List<StatementSource> sources = stmtToSources.get(stmt);
		synchronized (sources) {
			sources.add(source);
		}
	}
	/**
	 * Step 2 of HiBISCuS source selection. i.e. triple pattern-wise selected sources for hyperedge aka triple pattern
	 * @param dNFHyperVertices DNF groups (BGPs)of hypervertices
	 * @return Refine triple pattern-wise selected sources
	 */
	public Map<StatementPattern, List<StatementSource>> pruneSources(List<Set<Vertex>> dNFHyperVertices)
	{
		for (Set<Vertex> V : dNFHyperVertices)
		{
			//System.out.println("--------------new DNF Graph---------");
		 if(V.size()>3)  //---- only consider those DNF groups having atleast 2 triple patterns
		 {
		   for (Vertex v:V) //vertex
		    {
				Map <StatementPattern, Map<StatementSource, ArrayList<String>>> stmtToLstAuthorities = new HashMap <StatementPattern, Map<StatementSource, ArrayList<String>>>(); 
				ArrayList<String> authIntersectionSet = new ArrayList<String>();
				//---------------------------------------hybrid node-------------------------------------------------------------
			if((v.inEdges.size()>1 && v.outEdges.size()>0) || (v.inEdges.size()>0 && v.outEdges.size()>1)) 
			 {
				//System.out.println(v.label + " is hybrid node");
				for(HyperEdge inEdge: v.inEdges) //has hyperedges or statement patterns
				 {
					 Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
					 ArrayList<String> authUnionSet = new ArrayList<String>();
					 StatementPattern stmt =  hyperEdgeToStmt.get(inEdge);
					 for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
					 {
						ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
						authUnionSet = getUnion(authUnionSet, lstAuthorities);
						stmtSourceToAuthorities.put(src, lstAuthorities);
					 }
					 if(authIntersectionSet.size()==0)
							authIntersectionSet = authUnionSet;
						else
						 authIntersectionSet.retainAll(authUnionSet);
					    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
				 }
					for(HyperEdge outEdge: v.outEdges)
					{
						 Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
						 ArrayList<String> authUnionSet = new ArrayList<String>();
						 StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
						 for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
						 {
							ArrayList<String> lstAuthorities =  FedSumD_getMatchingSbjAuthorities(stmt,src);  //has authorities
						    authUnionSet = getUnion(authUnionSet, lstAuthorities);
							stmtSourceToAuthorities.put(src, lstAuthorities);
						 }
						 if(authIntersectionSet.size()==0)
								authIntersectionSet = authUnionSet;
							else
							 authIntersectionSet.retainAll(authUnionSet);
						    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
						 
					}
					
					 doSourcePrunning(stmtToLstAuthorities,authIntersectionSet);
			 }
			//---------------------------------------------star node--------------------------------------
			 else if( v.outEdges.size()>1) 
			 {
				// System.out.println(v.label + " is star node");
				 for(HyperEdge outEdge: v.outEdges) //has hyperedges or statement patterns
				 {
					 Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
					 ArrayList<String> authUnionSet = new ArrayList<String>();
					 StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
					 for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
					 {
						// long strt = System.currentTimeMillis();
						 ArrayList<String> lstAuthorities =  FedSumD_getMatchingSbjAuthorities(stmt,src);  //has authorities
						// System.out.println("loading sbj auth time : " + (System.currentTimeMillis()-strt));
						 authUnionSet = getUnion(authUnionSet, lstAuthorities);
						
						stmtSourceToAuthorities.put(src, lstAuthorities);
					 }
					 if(authIntersectionSet.size()==0)
							authIntersectionSet = authUnionSet;
						else
						 authIntersectionSet.retainAll(authUnionSet);
					    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
					// System.out.println("InEdge: "+inEdge.subj+", "+inEdge.pred+", "+inEdge.obj+"\nInEdges Sources: "+stmtToSources.get( hyperEdgeToStmt.get(inEdge)));
				 }
				 doSourcePrunning(stmtToLstAuthorities,authIntersectionSet);
			 }
		    //----------------------------------------Path node ----------------------------------
			 else if( v.outEdges.size()==1 && v.inEdges.size() == 1)
			 {
				 for(HyperEdge inEdge: v.inEdges) //has hyperedges or statement patterns
				 {
					//System.out.println(v.label + " is path node");
					 Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
					 ArrayList<String> authUnionSet = new ArrayList<String>();
					 StatementPattern stmt =  hyperEdgeToStmt.get(inEdge);
					 for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
					 {
						ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
						authUnionSet = getUnion(authUnionSet, lstAuthorities);
						stmtSourceToAuthorities.put(src, lstAuthorities);
					 }
					 if(authIntersectionSet.size()==0)
							authIntersectionSet = authUnionSet;
						else
						 authIntersectionSet.retainAll(authUnionSet);
					    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
				 }
					for(HyperEdge outEdge: v.outEdges)
					{
						 Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
						 ArrayList<String> authUnionSet = new ArrayList<String>();
						 StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
						 for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
						 {
							ArrayList<String> lstAuthorities =  FedSumD_getMatchingSbjAuthorities(stmt,src);  //has authorities
						    authUnionSet = getUnion(authUnionSet, lstAuthorities);
							stmtSourceToAuthorities.put(src, lstAuthorities);
						 }
						 if(authIntersectionSet.size()==0)
								authIntersectionSet = authUnionSet;
							else
							 authIntersectionSet.retainAll(authUnionSet);
						    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
						 
					}
					
					 doSourcePrunning(stmtToLstAuthorities,authIntersectionSet);
			 }
			  //-------------------sink node ----------------------------------------------------
			
			 else if( v.inEdges.size()>1 && v.outEdges.size()==0) 
			 {
				// System.out.println(v.label + " is sink node");
				 //--- cant do source pruning for for literal value sink node 
			 outerloop: 
				 for(HyperEdge inEdge: v.inEdges) //has hyperedges or statement patterns
				 {
					 Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
					 ArrayList<String> authUnionSet = new ArrayList<String>();
					 StatementPattern stmt =  hyperEdgeToStmt.get(inEdge);
					 for (StatementSource src:stmtToSources.get(stmt)) //has relevant sources
					 {
						ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
						if (lstAuthorities.isEmpty())
							break outerloop;
						authUnionSet = getUnion(authUnionSet, lstAuthorities);
						stmtSourceToAuthorities.put(src, lstAuthorities);
						
					 }
					 if(authIntersectionSet.size()==0)
							authIntersectionSet = authUnionSet;
						else
						 authIntersectionSet.retainAll(authUnionSet);
					    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
				 }
										
					 doSourcePrunning(stmtToLstAuthorities,authIntersectionSet);
				 }
			
			 
		 }
		 }
		 }
		return stmtToSources;
	}
	/**
	 *  Union of two Sets
	 * @param authUnionSet First Set
	 * @param lstAuthorities Second Set
	 * @return Union of two sets
	 */
	public ArrayList<String> getUnion(ArrayList<String> authUnionSet,	ArrayList<String> lstAuthorities)
	{
		for(String authority:lstAuthorities)
			if(!authUnionSet.contains(authority))
				authUnionSet.add(authority);
		return authUnionSet;
	}
/**
 * Remove irrelvant sources from each triple pattern according to step-2 of our source selection
 * @param stmtToLstAuthorities A map which stores the list of authorities for each capable source of a triple pattern
 * @param authIntersectionSet The common authorities set. see step 2 at FedSum paper for the usage of this list
 */
	private void doSourcePrunning(Map<StatementPattern, Map<StatementSource, ArrayList<String>>> stmtToLstAuthorities,ArrayList<String> authIntersectionSet) 
	{
		for(StatementPattern stmt:stmtToLstAuthorities.keySet())
		{
			Map<StatementSource, ArrayList<String>> stmtSourceToLstAuthorities = stmtToLstAuthorities.get(stmt);
			for(StatementSource src:stmtSourceToLstAuthorities.keySet())
			{
				ArrayList<String> srcAuthSet = stmtSourceToLstAuthorities.get(src);
				srcAuthSet.retainAll(authIntersectionSet);
				if(srcAuthSet.size()==0)
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
	 *  Get matching Subject authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @return List of authorities
	 * @throws RepositoryException Repository Exception
	 * @throws MalformedQueryException Memory Exception
	 * @throws QueryEvaluationException Query Exception
	 */

	public ArrayList<String> FedSumD_getMatchingSbjAuthorities(StatementPattern stmt, StatementSource src) throws RepositoryException, MalformedQueryException, QueryEvaluationException 
	{
		String endPointUrl = "http://"+src.getEndpointID().replace("sparql_", "");
		       endPointUrl = endPointUrl.replace("_", "/");
		ArrayList<String> sbjAuthorities = new ArrayList<String>();
		
		  String  queryString = getFedSumSbjAuthLookupQuery(stmt, endPointUrl) ;
		  
		     TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		     TupleQueryResult result = tupleQuery.evaluate();
				   while(result.hasNext())
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
		String s,p,o, sa = "null", oa="null";
		if (stmt.getSubjectVar().getValue()!=null)
		{
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa =sbjPrts[0]+"//"+sbjPrts[2];
				}
		else
		    	s ="null";  
		
		if (stmt.getPredicateVar().getValue()!=null)
		p = stmt.getPredicateVar().getValue().stringValue();
		else
			p ="null"; 
		
		if (stmt.getObjectVar().getValue()!=null)
		{
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if ((objPrts.length>1))      //add only URI
				oa =objPrts[0]+"//"+objPrts[2];
	         else
	        	 oa = "null";
			}
		else
			o = "null"; 
		//------------------------------------------------
		if(!p.equals("null")) // if predicate is bound
		{
		 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
			   		+ "SELECT  DISTINCT ?sbjAuth "
				   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
					+ " 		?s ds:capability ?cap."
					+ "        ?cap ds:predicate <"+p+">."
				  		     + "?cap ds:sbjAuthority  ?sbjAuth. }" ;	
		}
		else //predicate is not bound
		{
			if(sa.equals("null") && oa.equals("null"))  //and subject , object are not bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?sbjAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:sbjAuthority  ?sbjAuth. }" ;
			}
			else if(!sa.equals("null") && !oa.equals("null")) //and subject, object are bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?sbjAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:sbjAuthority  ?sbjAuth. "
							         + "?cap ds:objAuthority <"+oa+">. "
							         + "FILTER REGEX (str(?sbjAuth),'"+sa+"')"
					           + "}" ;
			}
			else if(!sa.equals("null") && oa.equals("null")) //and subject only is bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?sbjAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:sbjAuthority  ?sbjAuth. "
							         + "FILTER REGEX (str(?sbjAuth),'"+sa+"')"
					           + "}" ;
			}
			else if(sa.equals("null") && !oa.equals("null")) //and object is  bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?sbjAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:sbjAuthority  ?sbjAuth. "
							         + "?cap ds:objAuthority <"+oa+">. "
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
	   if (stmt.getPredicateVar().getValue()!=null)
			p = stmt.getPredicateVar().getValue().stringValue();
			else
				p =stmt.getPredicateVar().getName().toString(); 
	   String  queryString = getFedSumObjAuthLookupQuery(stmt, endPointUrl,v) ;
	    TupleQuery tupleQuery = QuetzalConfig.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		 TupleQueryResult result = tupleQuery.evaluate();
		 if(p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) //for rdf:type
			 
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
	public String getFedSumObjAuthLookupQuery(StatementPattern stmt,String endPointUrl, Vertex v)
	{
		String queryString = null;
		String s,p,o, sa = "null", oa="null";
		if (stmt.getSubjectVar().getValue()!=null)
		{
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa =sbjPrts[0]+"//"+sbjPrts[2];
				}
		else
		    	s ="null";  
		
		if (stmt.getPredicateVar().getValue()!=null)
		p = stmt.getPredicateVar().getValue().stringValue();
		else
		{
			if(stmt.getPredicateVar().getName().equals(v.label))
				p = stmt.getPredicateVar().getName();
			else
			p ="null"; 
		}
		
		if (stmt.getObjectVar().getValue()!=null)
		{
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if ((objPrts.length>1))      //add only URI
				oa =objPrts[0]+"//"+objPrts[2];
	         else
	        	 oa = "null";
			}
		else
			o = "null"; 
		//---------------------------You can enable this code for more advance join ware source selection---------------------
//	if(v.label.equals(p))   // if vertex is predicate of a triple pattern 
//	{
//		if(p.startsWith("http://") || p.startsWith("ftp://")) // if predicate is bound. Note this needs to be properly validated using UrlValidator class may be. 
//		{
//		 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
//			   		+ "SELECT DISTINCT  ?predAuth "
//				   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
//					+ " 		?s ds:capability ?cap."
//					+ "        ?cap ds:predicate ?predAuth."
//					+ "FILTER REGEX (str(?predAuth), '"+p+"')"
//				  		     + "}" ;	
//		}
//		else //predicate is not bound
//		{
//			if(sa.equals("null") && oa.equals("null"))  //and subject , object are not bound
//			{
//				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
//					   		+ "SELECT  Distinct ?predAuth "
//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
//							+ " 		?s ds:capability ?cap."
//							         + "?cap ds:predicate ?predAuth. }" ;
//			}
//			else if(!sa.equals("null") && !oa.equals("null")) //and subject, object are bound
//			{
//				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
//					   		+ "SELECT  Distinct ?predAuth "
//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
//							+ " 		?s ds:capability ?cap."
//							         + "?cap ds:predicate ?predAuth. "
//							         + "?cap ds:objAuthority <"+oa+">. "
//							         + "?cap ds:sbjAuthority <"+sa+">. "
//					           + "}" ;
//			}
//			else if(sa.equals("null") && !oa.equals("null")) //and object only is bound
//			{
//				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
//					   		+ "SELECT  Distinct ?predAuth "
//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
//							+ " 		?s ds:capability ?cap."
//							         + "?cap ds:predicate ?predAuth. "
//							         + "?cap ds:objAuthority <"+oa+">. "
//					           + "}" ;
//			}
//			else if(!sa.equals("null") && oa.equals("null")) //and subject is  bound
//			{
//				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
//					   		+ "SELECT  Distinct ?predAuth "
//						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
//							+ " 		?s ds:capability ?cap."
//							         + "?cap ds:predicate ?predAuth.  "
//							         + "?cap ds:sbjAuthority <"+sa+">. "
//							         + "}" ;
//			}
//		}
//	}
//	//-------------------
//	else
//	{
	if(!p.equals("null") && !stmt.getPredicateVar().getName().equals(v.label)) // if predicate is bound
		{
		 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
			   		+ "SELECT DISTINCT  ?objAuth "
				   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
					+ " 		?s ds:capability ?cap."
					+ "        ?cap ds:predicate <"+p+">."
				  		     + "?cap ds:objAuthority  ?objAuth. }" ;	
		}
		else //predicate is not bound
		{
			if(sa.equals("null") && oa.equals("null"))  //and subject , object are not bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?objAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:objAuthority  ?objAuth. }" ;
			}
			else if(!sa.equals("null") && !oa.equals("null")) //and subject, object are bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?objAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:objAuthority  ?objAuth. "
							         + "?cap ds:objAuthority <"+oa+">. "
							         + "FILTER REGEX (str(?objAuth),'"+oa+"')"
					           + "}" ;
			}
			else if(sa.equals("null") && !oa.equals("null")) //and object only is bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?objAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:objAuthority  ?objAuth. "
							         + "FILTER REGEX (str(?objAuth),'"+oa+"')"
					           + "}" ;
			}
			else if(!sa.equals("null") && oa.equals("null")) //and subject is  bound
			{
				 queryString = "Prefix ds:<http://aksw.org/fedsum/> "
					   		+ "SELECT  Distinct ?objAuth "
						   	+ " WHERE {?s ds:url <" + endPointUrl+">. "
							+ " 		?s ds:capability ?cap."
							         + "?cap ds:objAuthority  ?objAuth. "
							         + "?cap ds:sbjAuthority <"+sa+">. "
							         + "}" ;
			}
			
		}
	
							
		return queryString;
	}

	protected static class SourceSelectionExecutorWithLatch implements ParallelExecutor<BindingSet> {
		
		/**
		 * /**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch.
		 * Results are added to the map of the source selection instance. Errors 
		 * are reported as {@link OptimizationException} instances.
		 * @param hibiscusSourceSelection Source Selection
		 * @param tasks Set of SPARQL ASK tasks
		 * @param cache Cache
		 */
		public static void run(HibiscusSourceSelection hibiscusSourceSelection, List<CheckTaskPair> tasks, Cache cache) {
			new SourceSelectionExecutorWithLatch(hibiscusSourceSelection).executeRemoteSourceSelection(tasks, cache);
		}		
		
		private final HibiscusSourceSelection sourceSelection;
		private ControlledWorkerScheduler scheduler = FederationManager.getInstance().getScheduler();
		private CountDownLatch latch;
		private boolean finished = false;
		private Thread initiatorThread;
		protected List<Exception> errors = new ArrayList<Exception>();
		

		private SourceSelectionExecutorWithLatch(HibiscusSourceSelection hibiscusSourceSelection) {
			this.sourceSelection = hibiscusSourceSelection;
		}

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch
		 * 
		 * @param tasks
		 */
		private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
			if (tasks.size()==0)
				return;
			
			initiatorThread = Thread.currentThread();
			latch = new CountDownLatch(tasks.size());
			for (CheckTaskPair task : tasks)
				scheduler.schedule( new ParallelCheckTask(task.e, task.t, sourceSelection), QueryInfo.getPriority() + 1 );
			
			try	{
				latch.await(); 	// TODO maybe add timeout here
			} catch (InterruptedException e) {
				log.debug("Error during source selection. Thread got interrupted.");
			}

			finished = true;
			
			// check for errors:
			if (errors.size()>0) {
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

		@Override
		public void run() { /* not needed */ }

		@Override
		public void addResult(CloseableIteration<BindingSet, QueryEvaluationException> res)	{
			latch.countDown();
		}

		@Override
		public void toss(Exception e) {
			errors.add(e);
			scheduler.abort(getQueryId());	// abort all tasks belonging to this query id
			if (initiatorThread!=null)
				initiatorThread.interrupt();
		}

		@Override
		public void done()	{ /* not needed */ }

		@Override
		public boolean isFinished()	{
			return finished;
		}

		@Override
		public int getQueryId()	{
			return sourceSelection.queryInfo.getQueryID();
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
	protected static class ParallelCheckTask implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {

		final Endpoint endpoint;
		final StatementPattern stmt;
		final HibiscusSourceSelection sourceSelection;
		
		public ParallelCheckTask(Endpoint endpoint, StatementPattern stmt, HibiscusSourceSelection sourceSelection) {
			this.endpoint = endpoint;
			this.stmt = stmt;
			this.sourceSelection = sourceSelection;
		}

		
		@Override
		public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
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




