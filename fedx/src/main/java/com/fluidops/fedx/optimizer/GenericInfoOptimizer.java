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
import java.util.List;

import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.QueryInfo;


/**
 * Generic optimizer
 * 
 * Tasks:
 * - Collect information (hasUnion, hasFilter, hasService)
 * - Collect all statements in a list (for source selection), do not collect SERVICE expressions
 * - Collect all Join arguments and group them in the NJoin structure for easier optimization (flatten)
 * 
 * @author Andreas Schwarte
 */
public class GenericInfoOptimizer extends AbstractQueryModelVisitor<OptimizationException> implements FedXOptimizer {

	protected boolean hasFilter = false;
	protected boolean hasUnion = false;
	protected boolean hasService = false;
	
	//protected List<StatementPattern> stmts = new ArrayList<StatementPattern>();
	List<List<StatementPattern>> stmts = new ArrayList<List<StatementPattern>>();
	List<StatementPattern> curStmts;
	//private TupleExpr lastBGPNode;
	
	protected final QueryInfo queryInfo;
		
	public GenericInfoOptimizer(QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
		curStmts = new ArrayList<StatementPattern>();
		
	}

	public boolean hasFilter() {
		return hasFilter;
	}
	
	public boolean hasUnion() {
		return hasUnion;
	}
	
//	public List<StatementPattern> getStatements() {
//		return stmts;
//	}
	public List<List<StatementPattern>> getStatements() {
		return stmts;
	}
	
	@Override
	public void optimize(TupleExpr tupleExpr) {
		tupleExpr.visit(this);
		if (!curStmts.isEmpty()) {
			stmts.add(curStmts);
			curStmts = null;
		}
	}
	
	/////////////////////// BasicGraphPatternExtractor parts
	void beginNewBGPGroup() {
		if (!curStmts.isEmpty()) {
			stmts.add(curStmts);
			curStmts = new ArrayList<StatementPattern>();
		}
	}
	
	/**
	 * Handles binary nodes with potential BGPs as children (e.g. union, left join).
	 */
	@Override
	public void meetBinaryTupleOperator(BinaryTupleOperator node) {
		for (TupleExpr expr : new TupleExpr[] { node.getLeftArg(), node.getRightArg() }) {
			expr.visit(this);
			beginNewBGPGroup();
		}
	}
	
	/**
	 * Handles unary nodes with a potential BGP as child (e.g. projection).
	 */
	@Override
	public void meetUnaryTupleOperator(UnaryTupleOperator node) {
		node.getArg().visit(this);
		beginNewBGPGroup();
	}
	
	//////////////////////
	@Override
	public void meet(Union union) {
		hasUnion = true;
		super.meet(union);
	}
	
	@Override
	public void meet(Filter filter)  {
		hasFilter = true;
		//super.meet(filter); -> meetUnaryTupleOperator
		filter.getArg().visit(this);
		// no new bgp group
	}
	
	@Override
	public void meet(Service service) {
		hasService = true;
	}
	
	@Override
	public void meet(Join node) {
		
		/*
		 * Optimization task:
		 * 
		 * Collect all join arguments recursively and create the
		 * NJoin structure for easier join order optimization
		 */
				
		NJoin newJoin = OptimizerUtil.flattenJoin(node, queryInfo);
		int withoutStatementChildrenCount = 0;
		List<StatementPattern> accumPatterns = null;
		for (TupleExpr child : newJoin.getArgs())
		{
			child.visit(this);
			if (curStmts.isEmpty()) {
				++withoutStatementChildrenCount;
			} else if (accumPatterns == null) {
				accumPatterns = curStmts;
				curStmts = new ArrayList<StatementPattern>();
			} else {
				accumPatterns.addAll(curStmts);
				curStmts = new ArrayList<StatementPattern>();
			}
		}
		
		node.replaceWith(newJoin);
		curStmts = accumPatterns;
		if (withoutStatementChildrenCount != 0) {
			beginNewBGPGroup();
		}
	}
	
	@Override
	public void meet(StatementPattern node) {
		curStmts.add(node);
	}

	public boolean hasService()	{
		return hasService;
	}
}
