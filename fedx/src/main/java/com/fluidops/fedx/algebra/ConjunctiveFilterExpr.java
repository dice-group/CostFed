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
import java.util.List;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;


/**
 * ConjunctiveFilterExpr maintains a list of conjunctive (i.e. AND connected) constraints.
 * 
 * @author Andreas Schwarte
 *
 */
public class ConjunctiveFilterExpr extends AbstractQueryModelNode implements FilterValueExpr {
	private static final long serialVersionUID = -939167243579140948L;
	protected List<FilterExpr> expressions;
	
	public ConjunctiveFilterExpr(FilterExpr expr1, FilterExpr expr2) {
		this.expressions = new ArrayList<FilterExpr>(3);
		addExpression(expr1);
		addExpression(expr2);
	}
	
	public ConjunctiveFilterExpr(Collection<FilterExpr> expressions) {
		if (expressions.size()<2)
			throw new IllegalArgumentException("Conjunctive Expression must have at least two arguments.");
		this.expressions = new ArrayList<FilterExpr>(expressions.size());
		for (FilterExpr expr : expressions)
			addExpression(expr);
	}
	
	public void addExpression(FilterExpr expr) {
		// TODO use some priority ordering: selective filters should be evaluated first (shortcuts!)
		expressions.add(expr);
	}
	
	public List<FilterExpr> getExpressions() {
		return expressions;
	}
	
	@Override
	public ConjunctiveFilterExpr clone() {
		return (ConjunctiveFilterExpr)super.clone();
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);	
	}
	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
		throws X {
		super.visitChildren(visitor);
		for (FilterExpr expr : expressions)
			expr.getExpression().visit(visitor);
	}

}
