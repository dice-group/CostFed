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

import java.util.HashSet;

import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.AbstractQueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Compare.CompareOp;


/**
 * FilterExpr maintains information for a particular FILTER expression.
 * 
 * @author Andreas Schwarte
 *
 */
public class FilterExpr extends AbstractQueryModelNode implements FilterValueExpr {
	private static final long serialVersionUID = -7003970257782952219L;
	protected ValueExpr expr;
	protected HashSet<String> vars;

	public FilterExpr(ValueExpr expr, HashSet<String> vars) {
		super();
		this.expr = expr;
		this.vars = vars;
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
		expr.visit(visitor);
	}

	public ValueExpr getExpression() {
		return expr;
	}

	public HashSet<String> getVars() {
		return vars;
	}
	
	@Override
	public FilterExpr clone() {
		return (FilterExpr)super.clone();
	}
	
	public boolean isCompareEq() {
		return expr instanceof Compare && ((Compare)expr).getOperator()==CompareOp.EQ;
	}
}
