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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.iterator.SingleBindingSetIteration;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * A statement pattern with no free variables when provided with some particular BindingSet
 * in evaluate. For evaluation a boolean ASK query is performed.
 *  
 * Wraps a StatementTupleExpr
 * 
 * @author Andreas Schwarte
 */
public class CheckStatementPattern implements StatementTupleExpr, BoundJoinTupleExpr {
	private static final long serialVersionUID = 1543240924098875926L;
	protected final FedXConnection conn;
	protected final StatementTupleExpr stmt;
	
	public CheckStatementPattern(FedXConnection conn, StatementTupleExpr stmt) {
		super();
		this.conn = conn;
		this.stmt = stmt;
	}

	public StatementPattern getStatementPattern() {
		return (StatementPattern)stmt;
	}
	
	@Override
	public int getFreeVarCount() {
		return 0;
	}

	@Override
	public List<String> getFreeVars() {
		return Collections.emptyList();
	}

	@Override
	public int getId() {
		return 0;
	}

	@Override
	public List<StatementSource> getStatementSources() {
		return stmt.getStatementSources();
	}

	@Override
	public boolean hasFreeVarsFor(BindingSet binding) {
		return false;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		return stmt.getAssuredBindingNames();
	}

	@Override
	public Set<String> getBindingNames() {
		return stmt.getBindingNames();
	}

	@Override
	public QueryModelNode getParentNode() {
		return stmt.getParentNode();
	}

	@Override
	public String getSignature() {
		return stmt.getSignature();
	}

	@Override
	public void replaceChildNode(QueryModelNode current,
			QueryModelNode replacement) {
		stmt.replaceChildNode(current, replacement);
	}

	@Override
	public void replaceWith(QueryModelNode replacement) {
		stmt.replaceWith(replacement);		
	}

	@Override
	public void setParentNode(QueryModelNode parent) {
		stmt.setParentNode(parent);		
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		stmt.visit(visitor);		
	}

	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
			throws X {
		stmt.visitChildren(visitor);		
	}

	@Override
	public CheckStatementPattern clone() {
		throw new RuntimeException("Operation not supported on this node!");
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindings) {
		throw new Error("Not implemented: CheckStatementPattern.evaluate(List<BindingSet>)");
	}
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) {
		
		StatementPattern st = (StatementPattern)stmt;
	
		try {
			// return true if at least one endpoint has a result for this binding set
			for (StatementSource source : stmt.getStatementSources()) {
				Endpoint ownedEndpoint = conn.getEndpointManager().getEndpoint(source.getEndpointID());
				RepositoryConnection ownedConnection = ownedEndpoint.getConn();
				TripleSource t = ownedEndpoint.getTripleSource();
				if (t.hasStatements(st, ownedConnection, bindings)) {
					return new SingleBindingSetIteration(bindings);
				}
			}
		} catch (RepositoryException e) {
			throw new QueryEvaluationException(e);
		} catch (MalformedQueryException e) {
			throw new QueryEvaluationException(e);
		}
	
		// XXX return NULL instead and add an additional check?
		return new EmptyIteration<BindingSet, QueryEvaluationException>();
	}

	@Override
	public QueryInfo getQueryInfo() {
		return stmt.getQueryInfo();
	}

	@Override
	public void addLocalVar(String localVar) {
		stmt.addLocalVar(localVar);	
	}

	@Override
	public List<String> getLocalVars()
	{
		return stmt.getLocalVars();
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}
}
