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
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.algebra.EmptyNJoin;
import com.fluidops.fedx.algebra.EmptyResult;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.TrueStatementPattern;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.QueryStringUtil;


/**
 * Optimizer with the following tasks:
 * 
 * 1. Group {@link ExclusiveStatement} into {@link ExclusiveGroup}
 * 2. Adjust the join order using {@link JoinOrderOptimizer}
 * 
 * 
 * @author as
 */
public class StatementGroupOptimizer extends AbstractQueryModelVisitor<OptimizationException> implements FedXOptimizer {

	public static Logger log = LoggerFactory.getLogger(StatementGroupOptimizer.class);
	
	protected final QueryInfo queryInfo;
		
	public StatementGroupOptimizer(QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
	}

	@Override
	public void optimize(TupleExpr tupleExpr) {
		tupleExpr.visit(this);
	}

	@Override
	public void meet(Service tupleExpr) {
		// stop traversal
	}
	
	
	@Override
	public void meetOther(QueryModelNode node) {
		if (node instanceof NJoin) {
			super.meetOther(node);		// depth first
			meetNJoin((NJoin) node);
		} else {
			super.meetOther(node);
		}
	}

	
	protected void meetNJoin(NJoin node) {
		
		LinkedList<TupleExpr> newArgs = new LinkedList<TupleExpr>();
		
		LinkedList<TupleExpr> argsCopy = new LinkedList<TupleExpr>(node.getArgs());
		while (!argsCopy.isEmpty()) {
			
			TupleExpr t = argsCopy.removeFirst();
			
			/*
			 * If one of the join arguments cannot produce results,
			 * the whole join expression does not produce results.
			 * => replace with empty join and return
			 */
			if (t instanceof EmptyResult) {				
				node.replaceWith(new EmptyNJoin(node, queryInfo));
				return;
			}
			
			/*
			 * for exclusive statements find those belonging to the 
			 * same source (if any) and form exclusive group
			 */
			else if (t instanceof ExclusiveStatement) {
				ExclusiveStatement current = (ExclusiveStatement)t;
				
				List<ExclusiveStatement> l = null;
				for (TupleExpr te : argsCopy) {
					/* in the remaining join args find exclusive statements
					 * having the same source, and add to a list which is
					 * later used to form an exclusive group
					 */
					if (te instanceof ExclusiveStatement) {
						ExclusiveStatement check = (ExclusiveStatement)te;
						if (check.getOwner().equals(current.getOwner())) {
							if (l == null) {
								l = new ArrayList<ExclusiveStatement>();
								l.add(current);
							}
							l.add(check);
						}							
					}						
				}
				
				
				// check if we can construct a group, otherwise add directly
				if (l != null) {
					argsCopy.add(current); // will be removed in one row if pass checking
					checkExclusiveGroup(l);
					argsCopy.removeAll(l);
					newArgs.add(new ExclusiveGroup(l, current.getOwner(), queryInfo));
				} else {
					newArgs.add(current);
				}
			}
			
			/*
			 * statement yields true in any case, not needed for join
			 */
			else if (t instanceof TrueStatementPattern) {
				if (log.isDebugEnabled())
					log.debug("Statement " + QueryStringUtil.toString((StatementPattern)t) + " yields results for at least one provided source, prune it.");
			}
			
			else
				newArgs.add(t);
		}
		
		// if the join args could be reduced to just one, e.g. OwnedGroup
		// we can safely replace the join node
		if (newArgs.size() == 1) {
			log.debug("Join arguments could be reduced to a single argument, replacing join node.");
			node.replaceWith( newArgs.get(0) );
			return;
		}
		
		// in rare cases the join args can be reduced to 0, e.g. if all statements are 
		// TrueStatementPatterns. We can safely replace the join node in such case
		if (newArgs.isEmpty()) {
			log.debug("Join could be pruned as all join statements evaluate to true, replacing join with true node.");
			node.replaceWith( new TrueStatementPattern( new StatementPattern()));
			return;
		}
		
		List<TupleExpr> optimized = newArgs;
		
		// optimize the join order
		optimizeJoinOrder(node, optimized);
	}
	
	public void checkExclusiveGroup(List<ExclusiveStatement> exclusiveGroupStatements) {
		// by default do nothing
	}
	
	public void optimizeJoinOrder(NJoin node, List<TupleExpr> joinArgs) {
		List<TupleExpr> optimized = JoinOrderOptimizer.optimizeJoinOrder(joinArgs);
		// exchange the node
		NJoin newNode = new NJoin(optimized, queryInfo);
		node.replaceWith(newNode);
	}
}
