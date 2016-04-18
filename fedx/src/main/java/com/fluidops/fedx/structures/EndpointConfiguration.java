package com.fluidops.fedx.structures;

import com.fluidops.fedx.evaluation.SparqlTripleSource;

/**
 * Additional marker interface for Endpoint Configurations.
 * 
 * An {@link EndpointConfiguration} may bring additional 
 * configuration settings for an {@link Endpoint}, e.g.
 * in the case of a {@link SparqlTripleSource} it may
 * decide whether ASK or SELECT queries shall be used
 * for source selection.
 * 
 * @author Andreas Schwarte
 *
 */
public interface EndpointConfiguration {

}
