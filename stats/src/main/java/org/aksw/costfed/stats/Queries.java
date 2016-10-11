package org.aksw.costfed.stats;

/**
 * @author Tommaso Soru <tsoru@informatik.uni-leipzig.de>
 *
 */
public class Queries {
	
	public static final String GET_ALL_PROPERTIES = "select distinct ?x where { "
			+ "?s ?x ?o } limit 10000 offset %s";
	
	public static final String SUBJECT_FREQUENCIES = "select (count(?s) as ?x) where { "
			+ "?s <%s> ?o } group by ?o limit 10000 offset %s";
	
	public static final String OBJECT_FREQUENCIES = "select (count(?o) as ?x) where { "
			+ "?s <%s> ?o } group by ?s limit 10000 offset %s";
	

}
