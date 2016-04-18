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

package com.fluidops.fedx.evaluation;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.ExceptionConvertingIteration;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.IRI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.nativerdf.NativeStoreConnectionExt;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.evaluation.iterator.FilteringInsertBindingsIteration;
import com.fluidops.fedx.evaluation.iterator.FilteringIteration;
import com.fluidops.fedx.evaluation.iterator.InsertBindingsIteration;
import com.fluidops.fedx.evaluation.iterator.StatementConversionIteration;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.QueryAlgebraUtil;


/**
 * A triple source to be used on any repository.
 * 
 * @author Andreas Schwarte
 *
 */
public class SailTripleSource extends TripleSourceBase implements TripleSource {

	public static Logger log = Logger.getLogger(SailTripleSource.class);
	

	SailTripleSource(Endpoint endpoint) {
		super(FederationManager.getMonitoringService(), endpoint);
	}
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(
			String preparedQuery, RepositoryConnection conn, final BindingSet bindings, final FilterValueExpr filterExpr)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException {
		
		TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, preparedQuery, null);
		disableInference(query);		
		
		// evaluate the query
		CloseableIteration<BindingSet, QueryEvaluationException> res = query.evaluate();
		
		// apply filter and/or insert original bindings
		if (filterExpr!=null) {
			if (bindings.size()>0) 
				res = new FilteringInsertBindingsIteration(filterExpr, bindings, res);
			else
				res = new FilteringIteration(filterExpr, res);
			if (!res.hasNext())
				return new EmptyIteration<BindingSet, QueryEvaluationException>();
		} else if (bindings.size()>0) {
			res = new InsertBindingsIteration(res, bindings);
		}
		
		return res;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(
			StatementPattern stmt, RepositoryConnection conn,
			final BindingSet bindings, FilterValueExpr filterExpr)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException  {
	
		Value subjValue = QueryAlgebraUtil.getVarValue(stmt.getSubjectVar(), bindings);
		Value predValue = QueryAlgebraUtil.getVarValue(stmt.getPredicateVar(), bindings);
		Value objValue = QueryAlgebraUtil.getVarValue(stmt.getObjectVar(), bindings);			
		
		RepositoryResult<Statement> repoResult = conn.getStatements((Resource)subjValue, (IRI)predValue, objValue, true, new Resource[0]);
				
		// XXX implementation remark and TODO taken from Sesame
		// The same variable might have been used multiple times in this
		// StatementPattern, verify value equality in those cases.
		
		// an iterator that converts the statements to var bindings
		CloseableIteration<BindingSet, QueryEvaluationException> res = new StatementConversionIteration(repoResult, bindings, stmt);
				
		// if filter is set, apply it
		if (filterExpr != null) {
			CloseableIteration<BindingSet, QueryEvaluationException> fres = new FilteringIteration(filterExpr, res);
			if (!fres.hasNext())
				return new EmptyIteration<BindingSet, QueryEvaluationException>();
			return fres;
		}		
		
		return res;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> getStatements(RepositoryConnection conn, Resource subj, IRI pred, Value obj, Resource... contexts) throws RepositoryException,
			MalformedQueryException, QueryEvaluationException {
		
		// TODO add handling for contexts
		RepositoryResult<Statement> repoResult = conn.getStatements(subj, pred, obj, true);
		
		// XXX implementation remark and TODO taken from Sesame
		// The same variable might have been used multiple times in this
		// StatementPattern, verify value equality in those cases.
		
		return new ExceptionConvertingIteration<Statement, QueryEvaluationException>(repoResult) {
			@Override
			protected QueryEvaluationException convert(Exception arg0) {
				return new QueryEvaluationException(arg0);
			}
		};
	}
	
	
	@Override
	public boolean hasStatements(StatementPattern stmt,
			RepositoryConnection conn, BindingSet bindings)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException {

		Value subjValue = QueryAlgebraUtil.getVarValue(stmt.getSubjectVar(), bindings);
		Value predValue = QueryAlgebraUtil.getVarValue(stmt.getPredicateVar(), bindings);
		Value objValue = QueryAlgebraUtil.getVarValue(stmt.getObjectVar(), bindings);
		
		return conn.hasStatement((Resource)subjValue, (IRI)predValue, objValue, true, new Resource[0]);
	}

	@Override
	public boolean usePreparedQuery() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> getStatements(
			TupleExpr preparedQuery, RepositoryConnection conn,
			BindingSet bindings, FilterValueExpr filterExpr)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException {
		
		/*
		 * Implementation note:
		 * 
		 *  a hook is introduced for NativeStore instances such that an extended
		 *  connection is used. The extended connection provides a method to
		 *  evaluate prepared queries without prior (obsolete) optimization.  
		 */
	
		SailConnection sailConn = ((SailRepositoryConnection)conn).getSailConnection();
		
		CloseableIteration<BindingSet, QueryEvaluationException> res;
		if (sailConn instanceof NativeStoreConnectionExt) {
			NativeStoreConnectionExt _conn = (NativeStoreConnectionExt)sailConn;
			res = (CloseableIteration<BindingSet, QueryEvaluationException>) _conn.evaluatePrecompiled(preparedQuery);
		} else {
			try {
				log.warn("Precompiled query optimization for native store could not be applied: use extended NativeStore initialization using NativeStoreConnectionExt");
				res = (CloseableIteration<BindingSet, QueryEvaluationException>) sailConn.evaluate(preparedQuery, null, EmptyBindingSet.getInstance(), true);
			} catch (SailException e) {
				throw new QueryEvaluationException(e);
			}
		}
		
		if (bindings.size()>0) {
			res = new InsertBindingsIteration(res, bindings);
		}
		
		return res;
	}

	
}
