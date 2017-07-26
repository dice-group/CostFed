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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.repository.RepositoryException;

import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;

/**
 * Represents a group of statements that can only produce results at a single endpoint, the owner.
 * 
 * @author Andreas Schwarte
 *
 */
public class ExclusiveGroup extends AbstractQueryModelNode implements StatementTupleExpr, FilterTuple {
	private static final long serialVersionUID = -1980603360465355720L;
	protected final List<ExclusiveStatement> owned = new ArrayList<ExclusiveStatement>();
	protected final ArrayList<StatementSource> owner;
	protected final Set<String> freeVars = new HashSet<String>();
	protected final List<String> localVars = new ArrayList<String>();
	protected final int id;
	protected final QueryInfo queryInfo;
	protected FilterValueExpr filter = null;
	protected Endpoint ownedEndpoint = null;
	
	public ExclusiveGroup(Collection<ExclusiveStatement> ownedNodes, StatementSource owner, QueryInfo queryInfo) {
		owned.addAll(ownedNodes);
		this.owner = new ArrayList<StatementSource>(1);
		this.owner.add(owner);
		init();	// init free vars + filter expr
		this.id = NodeFactory.getNextId();
		this.queryInfo = queryInfo;
		ownedEndpoint = queryInfo.getFedXConnection().getEndpointManager().getEndpoint(owner.getEndpointID());
		queryInfo.numSources.addAndGet(-ownedNodes.size() + 1);
		queryInfo.totalSources.addAndGet(-ownedNodes.size() + 1);
	}
	
	/**
	 * Initialize free variables and filter expressions for owned children.
	 */
	protected void init() {
		HashSet<FilterExpr> conjExpr = new HashSet<FilterExpr>();
		for (ExclusiveStatement o  : owned) {
			freeVars.addAll(o.getFreeVars());
			
			if (o.hasFilter()) {
				
				FilterValueExpr expr = o.getFilterExpr();
				if (expr instanceof ConjunctiveFilterExpr) 
					conjExpr.addAll( ((ConjunctiveFilterExpr)expr).getExpressions());
				else if (expr instanceof FilterExpr)
					conjExpr.add((FilterExpr)expr);
				else 
					throw new RuntimeException("Internal Error: Unexpected filter type: " + expr.getClass().getSimpleName());
			}
		}
		
		if (conjExpr.size()==1)
			filter = conjExpr.iterator().next();
		else if (conjExpr.size()>1){
			filter = new ConjunctiveFilterExpr(conjExpr);
		}
	}

	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
		throws X {
		
		for (ExclusiveStatement s : owned)
			s.visit(visitor);
		if (localVars.size()>0)
			LocalVarsNode.visit(visitor, localVars);
	}
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);
	}
	
	@Override
	public Set<String> getAssuredBindingNames() {
		return Collections.emptySet();
	}

	@Override
	public Set<String> getBindingNames() {
		return freeVars;
		//return Collections.emptySet();
	}
		
	@Override
	public ExclusiveGroup clone() {
		throw new RuntimeException("Operation not supported on this node!");
	}
	
	public StatementSource getOwner() {
		return owner.get(0);
	}
	
	public List<ExclusiveStatement> getStatements() {
		// XXX make a copy? (or copyOnWrite list?)
		return owned;
	}
	
	@Override
	public int getFreeVarCount() {
		return freeVars.size();
	}
	
	public Set<String> getFreeVarsSet() {
		return freeVars;
	}
	
	@Override
	public List<String> getFreeVars() {
		return new ArrayList<String>(freeVars);
	}
	
	@Override
	public int getId() {
		return id;
	}

	@Override
	public List<StatementSource> getStatementSources() {
		return owner;
	}

	@Override
	public boolean hasFreeVarsFor(BindingSet bindings) {
		for (String var : freeVars)
			if (!bindings.hasBinding(var))
				return true;
		return false;		
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) {
		
		try {
			// use the particular evaluation strategy for evaluation
			return queryInfo.getFedXConnection().getStrategy().evaluateExclusiveGroup(this, ownedEndpoint.getConn(), ownedEndpoint.getTripleSource(), bindings);
		} catch (RepositoryException e) {
			throw new QueryEvaluationException(e);
		} catch (MalformedQueryException e) {
			throw new QueryEvaluationException(e);
		}
		
	}

	@Override
	public void addFilterExpr(FilterExpr expr) {
		/* 
		 * Note: the operation is obsolete for this class: all filters are added already
		 * in the owned children during optimization (c.f. FilterOptimizer)
		 */
		throw new UnsupportedOperationException("Operation not supported for " + ExclusiveGroup.class.getCanonicalName() + ", filters already to children during optimization.");
			
	}

	@Override
	public FilterValueExpr getFilterExpr() {
		return filter;
	}

	@Override
	public boolean hasFilter() {
		return filter!=null;
	}
	
	@Override
	public void addBoundFilter(final String varName, final Value value) {
		/* 
		 * Note: the operation is obsolete for this class: all bindings are set already
		 * in the owned children during optimization (c.f. FilterOptimizer)
		 */
		throw new UnsupportedOperationException("Operation not supported for " + ExclusiveGroup.class.getCanonicalName() + ", bindings inserted during optimization.");
	}

	@Override
	public QueryInfo getQueryInfo() {
		return this.queryInfo;
	}	

	@Override
	public void addLocalVar(String localVar) {
		this.localVars.add(localVar);		
	}

	@Override
	public List<String> getLocalVars() {
		return localVars;
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindings) {
		throw new Error("Not implemented: ExclusiveGroup.evaluate(List<BindingSet>)");
	}
}
