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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.AbstractQueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.QueryInfo;

public class IndependentJoinGroup extends AbstractQueryModelNode implements TupleExpr, FedXExpr, QueryRef, BoundJoinTupleExpr {
	private static final long serialVersionUID = 7239928475733942410L;
	protected final List<StatementTupleExpr> members;
	protected final QueryInfo queryInfo;
		
	public IndependentJoinGroup(List<StatementTupleExpr> members, QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
		this.members = members;
	}
	
	public IndependentJoinGroup(StatementTupleExpr stmt_a, StatementTupleExpr stmt_b, QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
		this.members = new ArrayList<StatementTupleExpr>(2);
		this.members.add(stmt_a);
		this.members.add(stmt_b);
	}
	

	public List<StatementTupleExpr> getMembers() {
		return members;
	}
	
	public int getMemberCount() {
		return members.size();
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		return Collections.emptySet();
	}

	@Override
	public Set<String> getBindingNames() {
		return Collections.emptySet();
	}

	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
		throws X {
		
		for (StatementTupleExpr s : members)
			s.visit(visitor);
	}
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);
	}
	
	@Override
	public IndependentJoinGroup clone() {
		throw new RuntimeException("Operation not supported on this node!");
	}

	@Override
	public QueryInfo getQueryInfo()	{
		return queryInfo;
	}

	@Override
	public void visit(FedXExprVisitor v) {
		v.meet(this);
	}
}
