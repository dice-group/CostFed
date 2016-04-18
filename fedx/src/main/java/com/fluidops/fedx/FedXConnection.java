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

package com.fluidops.fedx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.model.IRI;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.AbstractSail;
import org.openrdf.sail.helpers.AbstractSailConnection;

import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.iterator.RepositoryExceptionConvertingIteration;
import com.fluidops.fedx.evaluation.union.ControlledWorkerUnion;
import com.fluidops.fedx.evaluation.union.WorkerUnionBase;
import com.fluidops.fedx.optimizer.Optimizer;
import com.fluidops.fedx.sail.FedXSailRepositoryConnection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.DistinctIteration;
import info.aduna.iteration.ExceptionConvertingIteration;

/**
 * An implementation of RepositoryConnection that uses {@link FederationEvalStrategy} to evaluate 
 * provided queries. Prior to evaluation various optimizations are performed, see {@link Optimizer}
 * for further details.
 * 
 * Implementation notes:
 *  - the federation connection currently is read only
 *  - not all methods are implemented as of now
 * 
 * @author Andreas Schwarte
 *
 */
public class FedXConnection extends AbstractSailConnection {
	
	public static Logger log = Logger.getLogger(FedXConnection.class);
	protected FedX federation;
	
	public FedXConnection(FedX federation)
			throws SailException {
		super(new SailBaseDefaultImpl());
		this.federation = federation;
	}

	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
			TupleExpr query, Dataset dataset, BindingSet bindings,
			boolean includeInferred)
	{
		FederationEvalStrategy strategy = FederationManager.getInstance().getStrategy();

		long start=0;
		if (true) {
			if (log.isDebugEnabled()) {
				log.debug("Optimization start");
				start = System.currentTimeMillis();
			}
			try {
				String queryString = getOriginalQueryString(bindings);
				if (queryString==null)
					logger.warn("Query string is null. Please check your FedX setup.");
				QueryInfo queryInfo = new QueryInfo(queryString, getOriginalQueryType(bindings));
				FederationManager.getMonitoringService().monitorQuery(queryInfo);
				query = Optimizer.optimize(query, dataset, bindings, strategy, queryInfo);
			}  catch (Exception e) {
				log.error("Exception occured during optimization.", e);
				throw new SailException(e);
			}
			if (log.isDebugEnabled())
				log.debug(("Optimization duration: " + ((System.currentTimeMillis()-start))));
		}

		// log the optimized query plan, if Config#isLogQueryPlan(), otherwise void operation
		FederationManager.getMonitoringService().logQueryPlan(query);
		
		if (Config.getConfig().isDebugQueryPlan()) {
			System.out.println("Optimized query execution plan: \n" + query);
			log.debug("Optimized query execution plan: \n" + query);
		}
		
		try {
			return strategy.evaluate(query, EmptyBindingSet.getInstance());
		} catch (QueryEvaluationException e) {
			throw new SailException(e);
		} 		
	}

	
	@Override
	protected void clearInternal(Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("Operation is not yet supported.");		
	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		throw new UnsupportedOperationException("Operation is not yet supported.");		
	}

	@Override
	protected void closeInternal() throws SailException {
				
		/* think about it: the federation connection should remain open until the federation is shutdown. we
		 * use a singleton connection!!
		 */
	}
	
	@Override
	protected void commitInternal() throws SailException {
		throw new UnsupportedOperationException("Writing not supported to a federation: the federation is readonly.");
	}


	@Override
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() {
		final WorkerUnionBase<Resource> union = new ControlledWorkerUnion<Resource>(
				FederationManager.getInstance().getScheduler(),
				new QueryInfo("getContextIDsInternal", QueryType.UNKNOWN));	
		
		for (final Endpoint e : federation.getMembers()) {
			union.addTask(new Callable<CloseableIteration<Resource, QueryEvaluationException>>() {
				@Override
				public CloseableIteration<Resource, QueryEvaluationException> call() {
					return new RepositoryExceptionConvertingIteration<Resource>(e.getConn().getContextIDs());
				}
			});
		}
		
		return new DistinctIteration<Resource, SailException>(new ExceptionConvertingIteration<Resource, SailException>(union) {
			@Override
			protected SailException convert(Exception e) {
				return new SailException(e);
			}			
		});
	}

	@Override
	protected String getNamespaceInternal(String prefix) throws SailException {
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
			throws SailException {
		throw new UnsupportedOperationException("Operation is not yet supported.");		
	}

	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource subj, IRI pred,
			Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		try {
			FederationEvalStrategy strategy = FederationManager.getInstance().getStrategy();
			QueryInfo queryInfo = new QueryInfo(subj, pred, obj);
			FederationManager.getMonitoringService().monitorQuery(queryInfo);
			CloseableIteration<Statement, QueryEvaluationException> res = strategy.getStatements(queryInfo, subj, pred, obj, contexts);
			return new ExceptionConvertingIteration<Statement, SailException>(res) {
				@Override
				protected SailException convert(Exception e) {
					return new SailException(e);
				}
			};			
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new SailException(e);
		}
	}

	@Override
	protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("Not supported. the federation is readonly.");			
	}
	
	@Override
	protected void removeNamespaceInternal(String prefix) throws SailException {
		throw new UnsupportedOperationException("Not supported. the federation is readonly.");	
	}

	@Override
	protected void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("Not supported. the federation is readonly.");	
	}

	@Override
	protected void rollbackInternal() throws SailException {
		throw new UnsupportedOperationException("Not supported. the federation is readonly.");	
	}

	@Override
	protected void setNamespaceInternal(String prefix, String name) throws SailException {
		throw new UnsupportedOperationException("Not supported. the federation is readonly.");	
	}

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {
		if (contexts!=null && contexts.length>0)
			throw new UnsupportedOperationException("Context handling for size() not supported");
		long size = 0;
		List<String> errorEndpoints = new ArrayList<String>();
		for (Endpoint e : federation.getMembers()) {
			try	{
				size += e.size();
			} catch (RepositoryException e1){
				errorEndpoints.add(e.getId());
			}
		}
		if (errorEndpoints.size()>0)
			throw new SailException("Could not determine size for members " + errorEndpoints.toString() + 
					"(Supported for NativeStore and RemoteRepository only). Computed size: " +size);
		return size;
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		throw new UnsupportedOperationException("Not supported. the federation is readonly.");	
	}

	private static String getOriginalQueryString(BindingSet b) {
		if (b==null)
			return null;
		Value q = b.getValue(FedXSailRepositoryConnection.BINDING_ORIGINAL_QUERY);
		if (q!=null)
			return q.stringValue();
		return null;
	}
	
	private static QueryType getOriginalQueryType(BindingSet b) {
		if (b==null)
			return null;
		Value q = b.getValue(FedXSailRepositoryConnection.BINDING_ORIGINAL_QUERY_TYPE);
		if (q!=null)
			return QueryType.valueOf(q.stringValue());
		return null;
	}

	
	/**
	 * A default implementation for SailBase. This implementation has no 
	 * further use, however it is needed for the constructor call.
	 * 
	 * @author as
	 *
	 */
	protected static class SailBaseDefaultImpl extends AbstractSail {

		@Override
		protected SailConnection getConnectionInternal() throws SailException {
			return null;
		}

		@Override
		protected void shutDownInternal() throws SailException {
		}

		@Override
		public ValueFactory getValueFactory() {
			return null;
		}

		@Override
		public boolean isWritable() throws SailException {
			return false;
		}	
		
		@Override
		protected void connectionClosed(SailConnection connection) {
			// we do not need this in FedX
		}
	}
}
