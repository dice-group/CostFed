/*
 * Copyright (C) 2008-2013, fluid Operations AG
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.fluidops.fedx.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;

/**
 * Optimizer for SERVICE nodes.
 * 
 * @author Andreas Schwarte
 *
 */
public class ServiceOptimizer extends AbstractQueryModelVisitor<OptimizationException> implements FedXOptimizer {

	protected final QueryInfo queryInfo;
		
	/**
	 * @param queryInfo
	 */
	public ServiceOptimizer(QueryInfo queryInfo)
	{
		super();
		this.queryInfo = queryInfo;
	}


	@Override
	public void optimize(TupleExpr tupleExpr)
	{
		try { 
			tupleExpr.visit(this);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new FedXRuntimeException(e);
		}	
		
	}
	
	
	@Override
	public void meet(Service service) {
		// create an optimized service, which may wrap the original service
		// the latter is the case, if we cannot optimize the SERVICE node in FedX
		TupleExpr newExpr = optimizeService(service);
		service.replaceWith(newExpr);
	}
	
	protected TupleExpr optimizeService(Service service) {
		
		// check if there is a service uri given
		if (service.getServiceRef().hasValue()) {
			
			String serviceUri = service.getServiceRef().getValue().stringValue();
			
			GenericInfoOptimizer serviceInfo = new GenericInfoOptimizer(queryInfo);
			serviceInfo.optimize(service.getServiceExpr());
			
			Endpoint e = getFedXEndpoint(serviceUri);
			
			// endpoint is not in federation
			if (e == null) {
				// leave service as is, evaluate with Sesame code
				return new FedXService(service, queryInfo);
			} 
			
			StatementSource source = new StatementSource(e.getId(), StatementSourceType.REMOTE);
			List<ExclusiveStatement> stmts = new ArrayList<ExclusiveStatement>();
			// convert all statements to exclusive statements
			for (List<StatementPattern> sts : serviceInfo.getStatements()) {
				for (StatementPattern st : sts) {				
					ExclusiveStatement est = new ExclusiveStatement(st, source, queryInfo);
					st.replaceWith(est);
					stmts.add(est);
				}
			}
			
			// check if we have a simple subquery now (i.e. only a simple BGP)
			if (service.getArg() instanceof NJoin) {
				NJoin j = (NJoin)service.getArg();
				boolean simple = true;
				for (TupleExpr t : j.getArgs()) {
					if (!(t instanceof ExclusiveStatement)) {
						simple = false;
						break;
					}
				}
				
				if (simple) {
					return new ExclusiveGroup(stmts, source, queryInfo);
				}					
			}		
			
		}
		
		return new FedXService(service, queryInfo);
	}
	
	
	/**
	 * Return the FedX endpoint corresponding to the given service URI. If 
	 * there is no such endpoint in FedX, this method returns null.
	 * 
	 * Note that this method compares the endpoint URL first, however, that
	 * the name of the endpoint can be used as identifier as well. Note that
	 * the name must be a valid URI, i.e. start with http://
	 * 
	 * @param serviceUri
	 * @return
	 */
	private Endpoint getFedXEndpoint(String serviceUri) {
		EndpointManager em = EndpointManager.getEndpointManager();
		Endpoint e = em.getEndpointByUrl(serviceUri);
		if (e!=null)
			return e;
		e = em.getEndpointByName(serviceUri);
		return e;
	}
	
	
}


