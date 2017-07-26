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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.QueryInfo;


/**
 * Base class for any nary-tuple expression
 * 
 * @author Andreas Schwarte
 * 
 * @see NJoin
 * @see NUnion
 */
public abstract class NTuple extends AbstractQueryModelNode implements TupleExpr, FedXExpr, QueryRef {
	private static final long serialVersionUID = -6409323491371782098L;
	protected final List<TupleExpr> args;
	protected final QueryInfo queryInfo;
	
	/**
	 * Construct an nary-tuple. Note that the parentNode of all arguments is
	 * set to this instance.
	 * 
	 * @param args
	 */
	public NTuple(List<TupleExpr> args, QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
		this.args = args;
		for (TupleExpr expr : args)
			expr.setParentNode(this);
	}
	
	public TupleExpr getArg(int i) {
		return args.get(i);
	}
	
	public List<TupleExpr> getArgs() {
		return args;
	}
	
	public int getNumberOfArguments() {
		return args.size();
	}
	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor) throws X {
		for (TupleExpr expr : args)
			expr.visit(visitor);
	}
	
	@Override
	public NTuple clone() {
		return (NTuple)super.clone();
	}
	
	@Override
	public Set<String> getAssuredBindingNames() {
		Set<String> res = new LinkedHashSet<String>(16);
		for (TupleExpr e : args) {
			res.addAll(e.getAssuredBindingNames());
		}
		return res;
	}

	@Override
	public Set<String> getBindingNames() {
		Set<String> res = new LinkedHashSet<String>(16);
		for (TupleExpr e : args) {
			res.addAll(e.getBindingNames());
		}
		return res;
	}

	@Override
	public void replaceChildNode(QueryModelNode current, QueryModelNode replacement) {
		int index = args.indexOf(current);

		if (index >= 0) 
			args.set(index, (TupleExpr)replacement);
		else 
			super.replaceChildNode(current, replacement);	
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);		
	}

	@Override
	public QueryInfo getQueryInfo() {
		return queryInfo;
	}
}
