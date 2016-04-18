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

package com.fluidops.fedx.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.StatementTupleExpr;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * Optimizer with the following tasks:
 * 
 * 1. Find the scope of variables within groups (and store information 
 *    within the node)
 * 
 * 
 * @author as
 */
public class VariableScopeOptimizer extends AbstractQueryModelVisitor<OptimizationException> implements FedXOptimizer {

	public static Logger log = Logger.getLogger(VariableScopeOptimizer.class);
	
	protected final QueryInfo queryInfo;
	protected final Set<String> globalVariables = new HashSet<String>();

	
	public VariableScopeOptimizer(QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
	}

	// TODO variables that are required in FILTER and OPTIONAL (i.e. outside of joins)
	

	@Override
	public void optimize(TupleExpr tupleExpr) {
		tupleExpr.visit(this);
	}

	@Override
	public void meet(Service tupleExpr) {
		// stop traversal
	}
		
	
	@Override
	public void meet(ProjectionElem node) throws OptimizationException
	{
		globalVariables.add(node.getSourceName());
		super.meet(node);
	}
	
	public void meet(Var var) throws OptimizationException {
		globalVariables.add(var.getName());
	}

	@Override
	public void meetOther(QueryModelNode node) {
		if (node instanceof StatementTupleExpr) {
			meetTupleExpression((StatementTupleExpr)node);			
		} else if (node instanceof NJoin) {
			meetNJoin((NJoin) node);
		} else {
			super.meetOther(node);
		}
	}

	
	protected void meetTupleExpression(StatementTupleExpr node) {
		
		// we only get here if this expression is a toplevel expression,
		// i.e. it is not part of a join
		for (String var : node.getFreeVars())
			if (!isProjection(var))
				node.addLocalVar(var);
	}
	
	protected void meetNJoin(NJoin node) {

		// map variable names to their parent expressions
		Map<String, List<StatementTupleExpr>> map = new HashMap<String, List<StatementTupleExpr>>();
				
		for (TupleExpr t : node.getArgs()) {
			
			// we can only deal with our expressions. In fact,
			// t should always be a StatementTupleExpr
			if (!(t instanceof StatementTupleExpr)) {
				log.warn("Encountered unexpected expressions type: " + t.getClass() + ", please report this.");
				return;
			}
			
			StatementTupleExpr st = (StatementTupleExpr)t;
			for (String var : st.getFreeVars()) {
				if (isProjection(var))
					continue;
				List<StatementTupleExpr> l = map.get(var);
				if (l==null) {
					l = new ArrayList<StatementTupleExpr>();
					map.put(var, l);
				}
				l.add(st);
			}			
		}
		
		// register the local vars to the particular expression
		for (Map.Entry<String, List<StatementTupleExpr>> e : map.entrySet()) {
			if (e.getValue().size()>1)
				continue;
			StatementTupleExpr st = e.getValue().get(0);		
			st.addLocalVar(e.getKey());
		}		
	}	
	
	private boolean isProjection(String var) {
		return globalVariables.contains(var);
	}
}
