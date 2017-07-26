package org.aksw.simba.quetsal.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.aksw.simba.quetsal.core.Cardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import com.fluidops.fedx.algebra.StatementSource;

@Deprecated
public class QuetzalSummary implements Summary {
	static Logger log = LoggerFactory.getLogger(QuetzalSummary.class);
	
	static class KeyTuple implements Comparable<KeyTuple> {
		String predicate;
		String prefix;
		
		KeyTuple(String predicate, String prefix) {
			this.predicate = predicate;
			this.prefix = prefix;
		}
		
		@Override
		public int compareTo(KeyTuple rhs) {
			if (predicate == null) {
				if (rhs.predicate != null) return -1;
			} else if (rhs.predicate == null) {
				return 1;
			}
			int res = predicate != null ? predicate.compareTo(rhs.predicate) : 0;
			if (res != 0) return res;
			return prefix.compareTo(rhs.prefix);
		}
	}
	
	static class UrlPredicateKey {
		String url;
		String predicate;
		
		UrlPredicateKey(String url, String predicate) {
			this.url = url;
			this.predicate = predicate;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
			result = prime * result + url.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			UrlPredicateKey other = (UrlPredicateKey) obj;
			if (predicate == null) {
				if (other.predicate != null)
					return false;
			} else if (!predicate.equals(other.predicate))
				return false;
			return url.equals(other.url);
		}
	}
	
	static class ValueTuple {
		Set<String> urls;
		
		ValueTuple(String url) {
			urls = new HashSet<String>();
			urls.add(url);
		}
		
		void add(String url) {
			urls.add(url);
		}
	}
	
	static class SbjObjAuths {
		SortedSet<String> sbjs = new TreeSet<String>();
		SortedSet<String> objs = new TreeSet<String>();
		String[] sbjArr = null;
		String[] objArr = null;
		
		static Set<String> getCut(SortedSet<String> coll, String[] arr, String pattern) {
			int startpos = Arrays.binarySearch(arr, pattern);
			if (startpos == -1 || (startpos < 0 && !pattern.startsWith(arr[-startpos - 2]))) return new TreeSet<String>();
			if (startpos < 0) {
				startpos = -startpos - 2;
			}
			
			int endpos = Arrays.binarySearch(arr, startpos, arr.length, pattern + Character.MAX_VALUE);
			assert(endpos < 0);
			endpos = -endpos - 1;
			return endpos == arr.length ?
					coll.tailSet(arr[startpos]) :
						coll.subSet(arr[startpos], arr[endpos]);
		}
		
		Set<String> getSbjsCut(String pattern) {
			return getCut(sbjs, sbjArr, pattern);
		}
		
		Set<String> getObjsCut(String pattern) {
			return getCut(objs, objArr, pattern);
		}
	}
	
	SortedMap<KeyTuple, ValueTuple> psbj = new TreeMap<KeyTuple, ValueTuple>();
	SortedMap<KeyTuple, ValueTuple> pobj = new TreeMap<KeyTuple, ValueTuple>();
	Map<String, ValueTuple> purls = new HashMap<String, ValueTuple>();
	Map<UrlPredicateKey, SbjObjAuths> urlToAuths = new HashMap<UrlPredicateKey, SbjObjAuths>();
	Map<String, Set<String>> urlToPredicates = new HashMap<String, Set<String>>();
	
	Map<String, String> endpoints = new HashMap<String, String>();
	Map<String, String> strings = new HashMap<String, String>();
	
	RepositoryConnection conn;
	
	public QuetzalSummary(RepositoryConnection conn) {
	    this.conn = conn;
		long start = System.currentTimeMillis();
		
		String sbjQueryString = "Prefix ds:<http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?val"
				+ " WHERE { ?s ds:url ?url. "
				+ " 		?s ds:capability ?cap. "
				+ "		    ?cap ds:predicate ?p."
				+ "			?cap ds:sbjPrefix ?val }" ;
		
		
		fillMap(conn, sbjQueryString, psbj, purls, true);
		
		String objQueryString = "Prefix ds:<http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?val"
				+ " WHERE { ?s ds:url ?url. "
				+ " 		?s ds:capability ?cap. "
				+ "		    ?cap ds:predicate ?p."
				+ "			?cap ds:objPrefix ?val }" ;
		
		fillMap(conn, objQueryString, pobj, null, false);
		
		for (SbjObjAuths so : urlToAuths.values()) {
			so.sbjArr = so.sbjs.toArray(new String[]{});
			so.objArr = so.objs.toArray(new String[]{});
		}
		
		log.info("QuetzalSummary generation time: " + (System.currentTimeMillis() - start) + " ms");
	}
	
	private <KT> void updateMap(Map<KT, ValueTuple> map, KT k, String url) {
		ValueTuple v = map.get(k);
		if (v == null) {
			v = new ValueTuple(url);
			map.put(k, v);
		} else {
			v.add(url);
		}
	}
	
	void updateSbjObjAuths(UrlPredicateKey urlk, String val, boolean isSbj) {
		SbjObjAuths sovals = urlToAuths.get(urlk);
		if (sovals == null) {
			sovals = new SbjObjAuths();
			urlToAuths.put(urlk, sovals);
		}
		if (isSbj) {
			sovals.sbjs.add(val);
		} else {
			sovals.objs.add(val);
		}
	}
	
	private String getString(String s) {
		String result = strings.get(s);
		if (result == null) {
			result = s;
			strings.put(result, result);
		}
		return result;
	}
	
	void fillMap(RepositoryConnection con, String query, SortedMap<KeyTuple, ValueTuple> map, Map<String, ValueTuple> purls, boolean isSbj) {
		TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
		TupleQueryResult result = tupleQuery.evaluate();
		
		while (result.hasNext())
		{
			BindingSet row = result.next(); 
			String endpoint = row.getValue("url").stringValue();
			String eid = endpoints.get(endpoint);
			if (eid == null) {
				eid = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
				endpoints.put(endpoint, eid);
			}
			
			String predicate = getString(row.getValue("p").stringValue());
			String val = getString(row.getValue("val").stringValue());
			KeyTuple kt = new KeyTuple(predicate, val);
			updateMap(map, kt, eid);
			KeyTuple kt0 = new KeyTuple(null, val);
			updateMap(map, kt0, eid);
			
			if (purls != null) {
				updateMap(purls, predicate, eid);
				Set<String> ps = urlToPredicates.get(eid);
				if (ps == null) {
					ps = new HashSet<String>();
					urlToPredicates.put(eid, ps);
				}
				ps.add(predicate);
			}
			
			updateSbjObjAuths(new UrlPredicateKey(eid, predicate), val, isSbj);
			//updateSbjObjAuths(new UrlPredicateKey(eid, null), val, isSbj);
		}
	}
	
	static Set<String> lookupIds(String p, String prefix, SortedMap<KeyTuple, ValueTuple> sm) {
		KeyTuple k = new KeyTuple(p, prefix);
		SortedMap<KeyTuple, ValueTuple> t = sm.tailMap(k);
		if (!t.isEmpty()) {
			Map.Entry<KeyTuple, ValueTuple> e = t.entrySet().iterator().next();
			if (e.getKey().prefix.startsWith(prefix)) {
				return e.getValue().urls;
			}
		}
		return new HashSet<String>(); // empty map
	}
	
	String asUrlValue(Value v) {
		return (v != null && v instanceof IRI) ? v.stringValue() : null;
	}
	
	@Override
	public Set<String> lookupSources(StatementPattern stmt) {
		String s = asUrlValue(stmt.getSubjectVar().getValue());
		String p = asUrlValue(stmt.getPredicateVar().getValue());
		String o = asUrlValue(stmt.getObjectVar().getValue());
		
		return lookupSources(s, p, o);
	}
	
	@Override
	public Set<String> lookupSources(String sa, String p, String oa) {
		if (sa == null && oa == null) {
			Set<String> result = new HashSet<String>();
			if (p == null) {
				result.addAll(endpoints.values());
			} else {
				ValueTuple v = purls.get(p);
				if (v != null) {
					result.addAll(v.urls);
				}
			}
			return result;
		}
		
		if (sa == null) { // oa != null
			if (!"http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p)) {
				return lookupIds(p, oa, pobj);
			} else {
				KeyTuple k = new KeyTuple(p, oa);
				ValueTuple v = pobj.get(k);
				return v == null ? new HashSet<String>() : v.urls;
			}
		}
		if (oa == null) { // sa != null
			return lookupIds(p, sa, psbj);
		}
		Set<String> sa_ids = lookupIds(p, sa, psbj);
		Set<String> oa_ids = null;
		if (!"http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p)) {
			oa_ids = lookupIds(p, oa, pobj);
		} else {
			KeyTuple k = new KeyTuple(p, oa);
			ValueTuple v = pobj.get(k);
			oa_ids = v == null ? new HashSet<String>() : v.urls;
		}
		
		Set<String> min = sa_ids.size() < oa_ids.size() ? sa_ids : oa_ids;
		Set<String> max = sa_ids.size() >= oa_ids.size() ? sa_ids : oa_ids;
		assert (min != max);
		
		Set<String> result = new HashSet<String>();
		for (String id : min) {
			if (max.contains(id)) {
				result.add(id);
			}
		}
		return result;
	}

	@Override
	public Set<String> lookupObjPrefixes(StatementPattern stmt, String eid) {
		String subject = null;
		if (stmt.getSubjectVar().getValue() != null)
		{
			subject = stmt.getSubjectVar().getValue().stringValue();
		}
		
		String object = null;
		if (stmt.getObjectVar().getValue() != null)
		{
			object = stmt.getObjectVar().getValue().stringValue();
			if (!object.startsWith("http://")) {
				object = null;
			}
		}
		
		if (stmt.getPredicateVar().getValue() != null) {
			String predicate = stmt.getPredicateVar().getValue().stringValue();
			SbjObjAuths val = urlToAuths.get(new UrlPredicateKey(eid, predicate));
			if (val == null) return null; // unknown source
			return doLookupObjAuths(val, subject, object);
		}
		throw new Error("Not implemented yet");
	}
	
	@Override
	public Set<String> lookupSbjPrefixes(StatementPattern stmt, String eid) {
		String subject = null;
		if (stmt.getSubjectVar().getValue() != null)
		{
			subject = stmt.getSubjectVar().getValue().stringValue();
		}
		
		String object = null;
		if (stmt.getObjectVar().getValue() != null)
		{
			object = stmt.getObjectVar().getValue().stringValue();
			if (!object.startsWith("http://")) {
				object = null;
			}
		}
		
		if (stmt.getPredicateVar().getValue() != null) {
			String predicate = stmt.getPredicateVar().getValue().stringValue();
			SbjObjAuths val = urlToAuths.get(new UrlPredicateKey(eid, predicate));
			if (val == null) return null; // unknown source
			return doLookupSbjAuths(val, subject, object);
		}
		Set<String> ps = urlToPredicates.get(eid);
		if (ps == null) return null; // unknown source
		
		Set<String> result = new HashSet<String>();
		for (String p : ps) {
			SbjObjAuths val = urlToAuths.get(new UrlPredicateKey(eid, p));
			assert(val != null);
			Set<String> pauths = doLookupSbjAuths(val, subject, object);
			result.addAll(pauths);
		}
		return result;
	}
	
	public Set<String> doLookupSbjAuths(SbjObjAuths val, String subject, String object) {
		if (object != null) {
			Set<String> objset = val.getObjsCut(object);
			if (objset.isEmpty()) return objset;
		}
		if (subject != null) {
			return val.getSbjsCut(subject);
		}
		return val.sbjs;
	}
	
	public Set<String> doLookupObjAuths(SbjObjAuths val, String subject, String object) {
		if (subject != null) {
			Set<String> sbjset = val.getSbjsCut(subject);
			if (sbjset.isEmpty()) return sbjset;
		}
		if (object != null) {
			return val.getObjsCut(object);
		}
		return val.objs;
	}
	
	@Override
	public long getTriplePatternCardinality(StatementPattern stmt, List<StatementSource> stmtSrces) {
		return Cardinality.getTriplePatternCardinalityOriginal(conn, stmt, stmtSrces);
	}
	
	@Override
	public double getTriplePatternObjectMVKoef(StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		return 1;
	}
	
	@Override
	public double getTriplePatternSubjectMVKoef(StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		return 1;
	}

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public RepositoryConnection getConnection() {
        return conn;
    }

    @Override
    public void shutDown() {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }
}
