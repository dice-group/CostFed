package com.fluidops.fedx.structures;

import com.fluidops.fedx.evaluation.TripleSource;

/**
 * Additional {@link EndpointConfiguration} for SPARQL endpoints.
 * 
 * @author Andreas Schwarte
 *
 */
public class SparqlEndpointConfiguration implements EndpointConfiguration {

	private boolean supportsASKQueries = true;
	
	/**
	 * Flag indicating whether ASK queries are supported. Specific
	 * {@link TripleSource} implementations may use this information
	 * to decide whether to use ASK or SELECT for source selection.
	 * 
	 * @return boolean indicating whether ASK queries are supported
	 */
	public boolean supportsASKQueries() {
		return supportsASKQueries;
	}
	
	/**
	 * Define whether this endpoint supports ASK queries.
	 * 
	 * @param flag
	 */
	public void setSupportsASKQueries(boolean flag) {
		this.supportsASKQueries = flag;
	}
}
