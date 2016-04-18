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

import java.util.Set;

import org.openrdf.query.algebra.AbstractQueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * A query which has a single relevant source. These queries can be sent entirely 
 * to the endpoint as-is.
 * 
 * @author Andreas Schwarte
 */
public class SingleSourceQuery extends AbstractQueryModelNode implements TupleExpr, FedXExpr, QueryRef
{
	private static final long serialVersionUID = 7726504219008608921L;
	private final TupleExpr parsedQuery;
	private final Endpoint source;
	private final QueryInfo queryInfo;
		
	/**
	 * @param parsedQuery
	 * @param source
	 * @param queryString
	 */
	public SingleSourceQuery(TupleExpr parsedQuery, Endpoint source,
			QueryInfo queryInfo)
	{
		super();
		this.parsedQuery = parsedQuery;
		this.source = source;
		this.queryInfo = queryInfo;
	}

	public Endpoint getSource()	{
		return source;
	}

	public String getQueryString()	{
		return queryInfo.getQuery();
	}	

	@Override
	public QueryInfo getQueryInfo()
	{
		return queryInfo;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X
	{
		visitor.meetOther(this);
	}	
	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
			throws X {
		parsedQuery.visit(visitor);
		super.visitChildren(visitor);
	}
	

	@Override
	public String getSignature() {
		return super.getSignature() + " @" + source.getId();
	}

	@Override
	public Set<String> getBindingNames()
	{
		return parsedQuery.getBindingNames();
	}

	@Override
	public Set<String> getAssuredBindingNames()
	{
		return parsedQuery.getAssuredBindingNames();
	}

	@Override
	public SingleSourceQuery clone() {
		return (SingleSourceQuery)super.clone();
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}
}
