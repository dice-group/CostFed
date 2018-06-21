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

package com.fluidops.fedx.algebra;

import java.util.List;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.iterator.SingleBindingSetIteration;
import com.fluidops.fedx.exception.IllegalQueryException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.Pair;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.QueryStringUtil;



/**
 * Represents a StatementPattern that can only produce results at a single endpoint, the owner.
 * 
 * @author Andreas Schwarte
 */
public class ExclusiveStatement extends FedXStatementPattern {
	private static final long serialVersionUID = 3290145471772314112L;

	public ExclusiveStatement(StatementPattern node, StatementSource owner, QueryInfo queryInfo) {
		super(node, queryInfo);
		addStatementSource(owner);
		queryInfo.numSources.incrementAndGet();
		queryInfo.totalSources.incrementAndGet();
	}	

	public StatementSource getOwner() {
		return getStatementSources().get(0);
	}	

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) {
		if (bindings == null) return new EmptyIteration<BindingSet, QueryEvaluationException>();
		
		Endpoint ownedEndpoint = getQueryInfo().getFedXConnection().getEndpointManager().getEndpoint(getOwner().getEndpointID());
		RepositoryConnection ownedConnection = ownedEndpoint.getConn();
		TripleSource t = ownedEndpoint.getTripleSource();
		
		/*
		 * Implementation note: for some endpoint types it is much more efficient to use prepared queries
		 * as there might be some overhead (obsolete optimization) in the native implementation. This
		 * is for instance the case for SPARQL connections. In contrast for NativeRepositories it is
		 * much more efficient to use getStatements(subj, pred, obj) instead of evaluating a prepared query.
		 */			
	
		if (t.usePreparedQuery()) {
			Pair<String, Boolean> preparedQuery;
			try {
				preparedQuery = QueryStringUtil.selectQueryString(this, bindings, filterExpr);
			} catch (IllegalQueryException e1) {
				// TODO there might be an issue with filters being evaluated => investigate
				/* all vars are bound, this must be handled as a check query, can occur in joins */
				if (t.hasStatements(this, ownedConnection, bindings))
					return new SingleBindingSetIteration(bindings);
				return new EmptyIteration<BindingSet, QueryEvaluationException>();
			}
					
			return t.getStatements(preparedQuery.getFirst(), ownedConnection, bindings, (preparedQuery.getSecond() ? null : filterExpr) );
		} else {
			return t.getStatements(this, ownedConnection, bindings, filterExpr);
		}
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}

	@Override
	protected CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings, List<StatementSource> sources) {
		return evaluate(bindings);
	}
}
