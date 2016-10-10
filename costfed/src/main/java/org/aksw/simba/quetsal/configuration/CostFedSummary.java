package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.openrdf.model.IRI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import com.fluidops.fedx.algebra.StatementSource;

public class CostFedSummary implements Summary {
	static Logger log = Logger.getLogger(CostFedSummary.class);
	
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

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
			result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
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
			KeyTuple other = (KeyTuple) obj;
			if (predicate == null) {
				if (other.predicate != null)
					return false;
			} else if (!predicate.equals(other.predicate))
				return false;
			if (prefix == null) {
				if (other.prefix != null)
					return false;
			} else if (!prefix.equals(other.prefix))
				return false;
			return true;
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
	
	static class FreqValue {
		long unique = 0;
		long card = 0;
		
		public FreqValue(long u, long c) {
			unique = u;
			card = c;
		}
	}
	
	static class Capability {
		Map<String, Long> topsbjs = new HashMap<String, Long>();
		Map<String, Long> topobjs = new HashMap<String, Long>();
		Set<String> middlesbjs = new HashSet<String>();
		Set<String> middleobjs = new HashSet<String>();
		long middlesbjcard = 0;
		long middleobjcard = 0;
		
		SortedMap<String, FreqValue> sbjs = new TreeMap<String, FreqValue>();
		SortedMap<String, FreqValue> objs = new TreeMap<String, FreqValue>();
		String[] sbjArr = null;
		String[] objArr = null;
		
		long tripleCount = 0;
		long objectCount = 0;
		long subjectCount = 0;
		
		//long totalObjectCount = 0;
		
		static SortedMap<String, FreqValue> getCut(SortedMap<String, FreqValue> coll, String[] arr, String pattern) {
			int startpos = Arrays.binarySearch(arr, pattern);
			if (startpos == -1 || (startpos < 0 && !pattern.startsWith(arr[-startpos - 2]))) return null;
			if (startpos < 0) {
				startpos = -startpos - 2;
			}
			
			int endpos = Arrays.binarySearch(arr, startpos, arr.length, pattern + Character.MAX_VALUE);
			assert(endpos < 0);
			endpos = -endpos - 1;
			return endpos == arr.length ?
					coll.tailMap(arr[startpos]) :
						coll.subMap(arr[startpos], arr[endpos]);
		}
		
		SortedMap<String, FreqValue> getSbjsCut(String pattern) {
			if (sbjArr == null) {
				sbjArr = sbjs.keySet().toArray(new String[]{});
			}
			return getCut(sbjs, sbjArr, pattern);
		}
		
		SortedMap<String, FreqValue> getObjsCut(String pattern) {
			if (objArr == null) {
				objArr = objs.keySet().toArray(new String[]{});
			}
			return getCut(objs, objArr, pattern);
		}
	}
	
	class EndpointCapability
	{
		long totalTriples = 0;
		long totalSubjects = 0;
		long totalObjects = 0;
		Map<String, Capability> caps = new HashMap<String, Capability>(); // predicate -> Capability 
		
		public void setTotalTriples(long val) { totalTriples = val; }
		public long getTotalTriples() { return totalTriples; }
		
		public void setTotalSubjects(long val) { totalSubjects = val; }
		public long getTotalSubjects() { return totalSubjects; }
		
		public void setTotalObjects(long val) { totalObjects = val; }
		public long getTotalObjects() { return totalObjects; }
		
		public Capability get(String predicate) { return caps.get(predicate); }
		public void put(String predicate, Capability cap) { caps.put(predicate, cap); }
		
		public Collection<Capability> caps() { return caps.values(); }
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
	
	String getEID(String endpoint) {
		String eid = endpoints.get(endpoint);
		if (eid == null) {
			eid = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
			endpoints.put(endpoint, eid);
		}
		return eid;
	}
	
	RepositoryConnection conn;
	
	// endpoint url -> endpoint id // auxiliary
	Map<String, String> endpoints = new HashMap<String, String>();
	
	// (predicate, prefix) -> {endpoint ids}
	SortedMap<KeyTuple, ValueTuple> psbj = new TreeMap<KeyTuple, ValueTuple>(Collections.reverseOrder());
	SortedMap<KeyTuple, ValueTuple> pobj = new TreeMap<KeyTuple, ValueTuple>(Collections.reverseOrder());
	
	// (predicate, iri) -> {endpoint ids}
	Map<KeyTuple, ValueTuple> ptopsbj = new HashMap<KeyTuple, ValueTuple>();
	Map<KeyTuple, ValueTuple> ptopobj = new HashMap<KeyTuple, ValueTuple>(); // for rdf:type it contains the full set of objects
		
	// predicate -> {endpoint ids}
	Map<String, ValueTuple> purls = new HashMap<String, ValueTuple>();
	
	// endpoint id -> predicate -> Capability 
	Map<String, EndpointCapability> urlCaps = new HashMap<String, EndpointCapability>();
	
	void buildPrefixes(String query, SortedMap<KeyTuple, ValueTuple> prefixMap, boolean isSbjPrefix) {
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext())
		{
			BindingSet row = result.next();
			
			String endpoint = row.getValue("url").stringValue();
			String predicate = row.getValue("p").stringValue();
			String prefix = row.getValue("prefix").stringValue();
			long unique = Long.parseLong(row.getValue("unique").stringValue());
			long card = Long.parseLong(row.getValue("card").stringValue());
			
			//log.info(predicate + " : " + prefix + " : " + card);
			
			String eid = getEID(endpoint);
			KeyTuple kt = new KeyTuple(predicate, prefix);
			updateMap(prefixMap, kt, eid);
			KeyTuple kt0 = new KeyTuple(null, prefix);
			updateMap(prefixMap, kt0, eid);
			
			updateCapabilityPrefix(eid, predicate, prefix, unique, card, isSbjPrefix);
		}
		result.close();
	}
	
	void buildTops(String query, Map<KeyTuple, ValueTuple> prefixMap, boolean isSubject) {
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext())
		{
			BindingSet row = result.next();
			
			String endpoint = row.getValue("url").stringValue();
			String predicate = row.getValue("p").stringValue();
			String iri = row.getValue("iri").stringValue();
			long card = Long.parseLong(row.getValue("card").stringValue());
			
			String eid = getEID(endpoint);
			Capability cap = getOrCreateCapability(eid, predicate);
			if (isSubject) {
				cap.topsbjs.put(iri, card);
			} else {
				cap.topobjs.put(iri, card);
			}
			//log.info(predicate + " : " + iri + " : " + card);
			
			KeyTuple kt = new KeyTuple(predicate, iri);
			updateMap(prefixMap, kt, eid);
			//KeyTuple kt0 = new KeyTuple(null, iri);
			//updateMap(prefixMap, kt0, eid);
		}
		result.close();
	}
	
	Capability getOrCreateCapability(String eid, String predicate) {
		EndpointCapability servCaps = urlCaps.get(eid);
		if (servCaps == null) {
			servCaps = new EndpointCapability();
			urlCaps.put(eid, servCaps);
		}
		Capability cap = servCaps.get(predicate);
		if (cap == null) {
			cap = new Capability();
			servCaps.put(predicate, cap);
		}
		return cap;
	}
	
	Capability getCapability(String eid, String predicate) {
		EndpointCapability servCaps = urlCaps.get(eid);
		if (servCaps == null) return null;
		return servCaps.get(predicate);
	}
	
	void updateCapabilityPrefix(String eid, String predicate, String val, long unique, long card, boolean isSbj) {
		Capability cap = getOrCreateCapability(eid, predicate);
		if (isSbj) {
			cap.sbjs.put(val, new FreqValue(unique, card));
		} else {
			cap.objs.put(val, new FreqValue(unique, card));
		}
	}
	
	public CostFedSummary(RepositoryConnection conn) {
		this.conn = conn;
		
		long start = System.currentTimeMillis();
		
		String endpointsCapsQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?totalSbj ?totalObj ?totalTriples"
				+ " WHERE { ?s ds:url ?url; ds:totalSbj ?totalSbj; ds:totalObj ?totalObj; ds:totalTriples ?totalTriples. }";
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, endpointsCapsQuery);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext())
		{
			BindingSet row = result.next();
			String endpoint = row.getValue("url").stringValue();
			long sc = Long.parseLong(row.getValue("totalSbj").stringValue());
			long oc = Long.parseLong(row.getValue("totalObj").stringValue());
			long tc = Long.parseLong(row.getValue("totalTriples").stringValue());
			
			String eid = getEID(endpoint);
			EndpointCapability ec = urlCaps.get(eid);
			if (ec == null) {
				ec = new EndpointCapability();
				urlCaps.put(eid, ec);
			}
			ec.setTotalSubjects(sc);
			ec.setTotalObjects(oc);
			ec.setTotalTriples(tc);
		}
		result.close();
		
		String servPredCapsQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?dsbj ?dobj ?tc"
				+ " WHERE { ?s ds:url ?url. "
				+ " 		?s ds:capability ?cap. "
				+ "		    ?cap ds:predicate ?p."
				+ "			?cap ds:distinctSbjs ?dsbj."
				+ "			?cap ds:distinctObjs ?dobj."
				+ "         ?cap ds:triples ?tc.}" ;
		
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, servPredCapsQuery);
		result = tupleQuery.evaluate();
		
		while (result.hasNext())
		{
			BindingSet row = result.next();
			
			String endpoint = row.getValue("url").stringValue();
			String predicate = row.getValue("p").stringValue();
			long dsbj = Long.parseLong(row.getValue("dsbj").stringValue());
			long dobj = Long.parseLong(row.getValue("dobj").stringValue());
			long tc = Long.parseLong(row.getValue("tc").stringValue());
			
			String eid = getEID(endpoint);
			updateMap(purls, predicate, eid);
			Capability cap = getOrCreateCapability(eid, predicate);
			cap.subjectCount = dsbj;
			cap.objectCount = dobj;
			cap.tripleCount = tc;
		}
		
		String subjPrefixesQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?prefix ?unique ?card"
				+ " WHERE { ?s ds:url ?url; ds:capability [ds:predicate ?p; ds:subjPrefixes [ds:prefix ?prefix; ds:unique ?unique; ds:card ?card]] }";
		buildPrefixes(subjPrefixesQuery, psbj, true);

		String objPrefixesQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?prefix ?unique ?card"
				+ " WHERE { ?s ds:url ?url; ds:capability [ds:predicate ?p; ds:objPrefixes [ds:prefix ?prefix; ds:unique ?unique; ds:card ?card]] }";
		buildPrefixes(objPrefixesQuery, pobj, false);
		
		String sbjTopQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?iri ?card"
				+ " WHERE { ?s ds:url ?url; ds:capability [ds:predicate ?p; ds:topSbjs [ds:subject ?iri; ds:card ?card]] }";
		buildTops(sbjTopQuery, ptopsbj, true);
		
		String objTopQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?iri ?card"
				+ " WHERE { ?s ds:url ?url; ds:capability [ds:predicate ?p; ds:topObjs [ds:object ?iri; ds:card ?card]] }"; 
		buildTops(objTopQuery, ptopobj, false);
		
		/*
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, subjPrefixesQuery);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext())
		{
			BindingSet row = result.next();
			
			String endpoint = row.getValue("url").stringValue();
			String predicate = row.getValue("p").stringValue();
			String prefix = row.getValue("prefix").stringValue();
			long unique = Long.parseLong(row.getValue("unique").stringValue());
			long card = Long.parseLong(row.getValue("card").stringValue());
			
			String eid = getEID(endpoint);
			KeyTuple kt = new KeyTuple(predicate, prefix);
			updateMap(psbj, kt, eid);
		}
		
		String servPredCapsQuery = "Prefix ds: <http://aksw.org/quetsal/> "
				+ "SELECT ?url ?p ?dsbj ?dobj ?tc"
				+ " WHERE { ?s ds:url ?url. "
				+ " 		?s ds:capability ?cap. "
				+ "		    ?cap ds:predicate ?p."
				+ "			?cap ds:distinctSbjs ?dsbj."
				+ "			?cap ds:distinctObjs ?dobj."
				+ "         ?cap ds:triples ?tc.}" ;
		
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, servPredCapsQuery);
		TupleQueryResult result = tupleQuery.evaluate();
		
		while (result.hasNext())
		{
			BindingSet row = result.next();
			
			String endpoint = row.getValue("url").stringValue();
			String predicate = row.getValue("p").stringValue();
			long dsbj = Long.parseLong(row.getValue("dsbj").stringValue());
			long dobj = Long.parseLong(row.getValue("dobj").stringValue());
			long tc = Long.parseLong(row.getValue("tc").stringValue());
			
			String eid = endpoints.get(endpoint);
			if (eid == null) {
				eid = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
				endpoints.put(endpoint, eid);
			}
			
			String subjPrefixesQuery = "Prefix ds: <http://aksw.org/quetsal/> "
					+ "SELECT ?url ?p ?prefix ?unique ?card"
					+ " WHERE { ?s ds:url ?url. "
					+ " 		?s ds:capability ?cap."
					+ "		    ?cap ds:predicate ?p."
					+ "			?cap ds:subjPrefixes ?sp."
					+ "			?sp ds:prefix ?prefix."
					+ "			?sp ds:unique ?unique."
					+ "			?sp ds:card ?card.";
			log.info(endpoint + ", " + predicate + ", " + dsbj + ", " + dobj + ", " + tc);
		}
		*/
		
		log.info("CostFedSummary generation time: " + (System.currentTimeMillis() - start) + " ms");
	}
	


	static Set<String> lookupIds(String p, String prefix, SortedMap<KeyTuple, ValueTuple> sm) {
		HashSet<String> result = new HashSet<String>();
		
		KeyTuple k = new KeyTuple(p, prefix);
		SortedMap<KeyTuple, ValueTuple> lessmap = sm.tailMap(k);
		for (Map.Entry<KeyTuple, ValueTuple> e : lessmap.entrySet())
		{
			if (!prefix.startsWith(e.getKey().prefix)) break;
			result.addAll(e.getValue().urls);
		}
		return result;
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
	public Set<String> lookupSources(String s, String p, String o) {
		if (s == null && o == null) {
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
		
		if (s == null) {
			assert (o != null);
			if (!"http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p)) {
				return lookupIds(p, o, pobj);
			} else {
				KeyTuple k = new KeyTuple(p, o);
				ValueTuple v = ptopobj.get(k);
				return v == null ? new HashSet<String>() : v.urls;
			}
		}
		if (o == null) {
			assert (s != null);
			return lookupIds(p, s, psbj);
		}
		Set<String> s_ids = lookupIds(p, s, psbj);
		Set<String> o_ids = null;
		if (!"http://www.w3.org/1999/02/22-rdf-syntax-ns#type".equals(p)) {
			o_ids = lookupIds(p, o, pobj);
		} else {
			KeyTuple k = new KeyTuple(p, o);
			ValueTuple v = ptopobj.get(k);
			o_ids = v == null ? new HashSet<String>() : v.urls;
		}
		
		Set<String> min = s_ids.size() < o_ids.size() ? s_ids : o_ids;
		Set<String> max = s_ids.size() >= o_ids.size() ? s_ids : o_ids;
		assert (min != max);
		
		Set<String> result = new HashSet<String>();
		for (String id : min) {
			if (max.contains(id)) {
				result.add(id);
			}
		}
		return result;
	}
	
	public Set<String> doLookupSbjPrefixes(Capability val, String predicate, String object) {
		if (object != null) {
			if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				if (!val.topobjs.containsKey(object)) return new HashSet<String>();
			} else {
				SortedMap<String, FreqValue> objset = val.getObjsCut(object);
				if (objset == null || objset.isEmpty()) return new HashSet<String>();
			}
		}
		//if (subject != null) {
		//	return val.getSbjsCut(subject);
		//}
		return val.sbjs.keySet();
	}
	
	public Set<String> doLookupObjPrefixes(Capability val, String subject) {
		if (subject != null) {
			SortedMap<String, FreqValue> sbjset = val.getSbjsCut(subject);
			if (sbjset == null || sbjset.isEmpty()) return null;
		}
		//if (object != null) {
		//	return val.getObjsCut(object);
		//}
		return val.objs.keySet();
	}
	
	@Override
	public Set<String> lookupSbjPrefixes(StatementPattern stmt, String eid) {
		String subject = asUrlValue(stmt.getSubjectVar().getValue());
		if (subject != null) throw new RuntimeException("internal error: subject is not null");
		String object = asUrlValue(stmt.getObjectVar().getValue());
		String predicate = asUrlValue(stmt.getPredicateVar().getValue());
		if (predicate != null) {
			Capability cap = getCapability(eid, predicate);
			if (cap == null) return null; // unknown source
			return doLookupSbjPrefixes(cap, predicate, object);
		}
		EndpointCapability ps = urlCaps.get(eid);
		if (ps == null) return null; // unknown source
		
		Set<String> result = new HashSet<String>();
		for (Map.Entry<String, Capability> e : ps.caps.entrySet()) {
			Set<String> prefixes = doLookupSbjPrefixes(e.getValue(), e.getKey(), object);
			result.addAll(prefixes);
		}
		return result;
	}
	
	@Override
	public Set<String> lookupObjPrefixes(StatementPattern stmt, String eid) {
		String subject = asUrlValue(stmt.getSubjectVar().getValue());
		String object = asUrlValue(stmt.getObjectVar().getValue());
		if (object != null) {
			throw new RuntimeException("internal error: object is not null");
		}
		String predicate = asUrlValue(stmt.getPredicateVar().getValue());
		if (predicate != null) {
			Capability cap = getCapability(eid, predicate);
			if (cap == null) return null; // unknown source
			return doLookupObjPrefixes(cap, subject);
		}

		EndpointCapability ps = urlCaps.get(eid);
		if (ps == null) return null; // unknown source
		
		Set<String> result = new HashSet<String>();
		for (Capability cap : ps.caps()) {
			Set<String> prefixes = doLookupObjPrefixes(cap, subject);
			result.addAll(prefixes);
		}
		return result;
	}
	
///////////////////////////////////////////////////////////////////////////////////////
	static String boundPredicate(StatementPattern stmt) {
		Value v = stmt.getPredicateVar().getValue();
		return v != null ? v.stringValue() : null;
	}
	
	static String boundSubject(StatementPattern stmt) {
		Value v = stmt.getSubjectVar().getValue();
		return v != null ? v.stringValue() : null;
	}
	
	static String boundObject(StatementPattern stmt) {
		Value v = stmt.getObjectVar().getValue();
		return v != null ? v.stringValue() : null;
	}
	
	long getOCard(Capability c, String o) {
		if (c == null) return 0;
		Long card = c.topobjs.get(o);
		if (card != null) {
			return card;
		}
		if (c.middleobjs.contains(o)) {
			return c.middleobjcard;
		}
		SortedMap<String, FreqValue> prefdescr = c.getObjsCut(o);
		if (prefdescr != null && !prefdescr.isEmpty()) {
			FreqValue fv = prefdescr.values().iterator().next(); // prefdescr can contain only 1 key-value pair
			return fv.card / fv.unique;
		}
		return c.tripleCount / c.objectCount;
	}
	
	long getSCard(Capability c, String s) {
		if (c == null) return 0;
		Long card = c.topsbjs.get(s);
		if (card != null) {
			return card;
		}
		if (c.middlesbjs.contains(s)) {
			return c.middlesbjcard;
		}
		SortedMap<String, FreqValue> prefdescr = c.getSbjsCut(s);
		if (prefdescr != null && !prefdescr.isEmpty()) {
			FreqValue fv = prefdescr.values().iterator().next(); // prefdescr can contain only 1 key-value pair
			return fv.card / fv.unique;
		}
		return c.tripleCount / c.subjectCount;
	}
	
	long getSOCard(Capability c, String s, String o) {
		if (s == null && o != null) { //?s <p> <o>
			return getOCard(c, o);
		} else if (s != null && o == null) { //<s> <p> ?o
			return getSCard(c, s);
		} else if (s == null && o == null) { // ?s <p> ?o
			return c.tripleCount;
		} else { // <s> <p> <o>
			return getSCard(c, s) > 0 && getOCard(c, o) > 0 ? 1 : 0;
		}
	}
	
	@Override
	public long getTriplePatternCardinality(StatementPattern stmt, List<StatementSource> stmtSrces) {
		long result = 0;
		String p = boundPredicate(stmt), s = boundSubject(stmt), o = boundObject(stmt);
		
		for (StatementSource ss : stmtSrces)
		{
			if (p == null) {
				if (s == null && o == null) { //?s ?p ?o
					EndpointCapability ec = urlCaps.get(ss.getEndpointID());
					result += ec == null ? (long)Integer.MAX_VALUE : ec.getTotalTriples();
				} else {
					EndpointCapability servCaps = urlCaps.get(ss.getEndpointID());
					for (Map.Entry<String, Capability> ent : servCaps.caps.entrySet())
					{
						result += getSOCard(ent.getValue(), s, o);
					}
				}
			} else {
				Capability c = getCapability(ss.getEndpointID(), p);
				if (c == null) continue;
				result += getSOCard(c, s, o);
			}
		}
		
		return result;
	}
	
	@Override
	public double getTriplePatternObjectMVKoef(StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		// for queries where there is a join by the subject of this pattern
		String s = boundSubject(stmt);
		String p = boundPredicate(stmt);
		String o = boundObject(stmt);
		if (p == null) {
			// not implemented
			return 1.0;
		}
		if (s != null || o != null) return 0.71;
		long totalObjects = 0, totalTriples = 0;
		for (StatementSource ss : stmtSrces)
		{
			Capability c = getCapability(ss.getEndpointID(), p);
			totalObjects += c.objectCount;
			totalTriples += c.tripleCount;
		}
		return ((double)totalTriples)/totalObjects;
	}
	
	@Override
	public double getTriplePatternSubjectMVKoef(StatementPattern stmt, List<StatementSource> stmtSrces)
	{
		// for queries where there is a join by the subject of this pattern
		String s = boundSubject(stmt);
		String p = boundPredicate(stmt);
		String o = boundObject(stmt);
		if (p == null) {
			// not implemented
			return 1.0;
		}
		if (s != null || o != null) return 0.71;
		long totalSubjects = 0, totalTriples = 0;
		for (StatementSource ss : stmtSrces)
		{
			Capability c = getCapability(ss.getEndpointID(), p);
			totalSubjects += c.subjectCount;
			totalTriples += c.tripleCount;
		}
		return ((double)totalTriples)/totalSubjects;
	}
	
///////////////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws IOException
	{
		String theFedSummaries = "summaries/sumX-localhost5.n3";
		Repository repo = new SailRepository( new MemoryStore(/*new File("summaries/")*/) );
		repo.initialize();
		RepositoryConnection conn = repo.getConnection();
		conn.add(new File(theFedSummaries), "aksw.org.simba", RDFFormat.N3);
		try {
			new CostFedSummary(conn);
			//TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
			//TupleQueryResult rs = query.evaluate();
			//return Long.parseLong(rs.next().getValue("objs").stringValue());
		} finally {
			conn.close();
			repo.shutDown();
		}
		
	}
}
