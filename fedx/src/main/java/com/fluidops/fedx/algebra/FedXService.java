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

import java.util.Collection;
import java.util.Set;

import org.openrdf.query.algebra.AbstractQueryModelNode;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.structures.QueryInfo;

public class FedXService extends AbstractQueryModelNode implements TupleExpr, FedXExpr, BoundJoinTupleExpr {
	private static final long serialVersionUID = -825303693177221091L;
	protected Service expr;
	protected QueryInfo queryInfo;
	protected boolean simple = true;		// consists of BGPs only
	protected int nTriples = 0;

	
	public FedXService(Service expr, QueryInfo queryInfo) {
		this.expr = expr;
		this.queryInfo = queryInfo;
		expr.visit(new ServiceAnalyzer());
	}

	
	public Service getService() {
		return this.expr;
	}
	
	public QueryInfo getQueryInfo() {
		return queryInfo;
	}
	
	public int getNumberOfTriplePatterns() {
		return nTriples;
	}
	
	public boolean isSimple() {
		return simple;
	}
	
	public Collection<String> getFreeVars() {
		return expr.getServiceVars();
	}
	
	public int getFreeVarCount() {
		return expr.getServiceVars().size();
	}
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);		
	}
	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor) throws X {
		expr.visit(visitor);
	}
	
	
	@Override
	public FedXService clone() {
		return (FedXService)super.clone();
	}


	@Override
	public Set<String> getAssuredBindingNames()
	{
		return expr.getAssuredBindingNames();
	}


	@Override
	public Set<String> getBindingNames()
	{
		return expr.getBindingNames();
	}	
	
	
	private class ServiceAnalyzer extends AbstractQueryModelVisitor<RuntimeException> {

		@Override
		protected void meetNode(QueryModelNode node)
		{
			if (node instanceof StatementTupleExpr) {
				nTriples++;
			} else if (node instanceof StatementPattern) {
				nTriples++;
			} else if (node instanceof Filter) {
				simple=false;
			} else if (node instanceof Union){
				simple=false;
			}
				
			super.meetNode(node);
		}		
		
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}
}
