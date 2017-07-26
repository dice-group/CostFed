package org.aksw.simba.quetsal.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.simba.quetsal.core.algebra.HashJoin;
import org.aksw.simba.quetsal.core.algebra.JoinRestarter;
import org.aksw.simba.quetsal.core.algebra.TopKSourceStatementPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.optimizer.OptimizerUtil;
import com.fluidops.fedx.structures.QueryInfo;

public class TopKJoinOrderOptimizer extends JoinOrderOptimizer {
	public static Logger log = LoggerFactory.getLogger(TopKJoinOrderOptimizer.class);
			
	public TopKJoinOrderOptimizer(QueryInfo queryInfo) {
		super(queryInfo);
	}

	class TopKEstimatorVisitor extends JoinOrderOptimizer.EstimatorVisitor {

		List<TopKSourceStatementPattern> topksrcs = new ArrayList<TopKSourceStatementPattern>();
		
		TupleExpr currentNode;
		
		TupleExpr getNode() {
			return currentNode;
		}
		
		public void meet(StatementSourcePattern stmt) {
			TopKSourceStatementPattern nd = new TopKSourceStatementPattern(stmt, queryInfo);
			currentNode = nd;
			current.card = nd.getCardinality(0);
			topksrcs.add(nd);
		}
		
		public void meet(ExclusiveGroup eg) {
			super.meet(eg);
			currentNode = eg;
			//log.info("meet ExclusiveGroup: " + eg);
		}
		
		public void meet(ExclusiveStatement eg) {
			super.meet(eg);
			currentNode = eg;
		}
		
		@Override
		protected void meetNode(QueryModelNode node) {
			if (node instanceof StatementSourcePattern) {
				meet((StatementSourcePattern)node);
			} else if (node instanceof ExclusiveGroup) {
				meet((ExclusiveGroup)node);
			} else if (node instanceof ExclusiveStatement) {
				meet((ExclusiveStatement)node);
			} else {
				throw new RuntimeException(node + "is not expected");
			}
		}
	}
	
	@Override
	public void optimizeJoinOrder(NJoin node, List<TupleExpr> joinArgs) {
		// check for limit clause
		boolean sliceWasFound = false;
		for (QueryModelNode pnd = node.getParentNode(); pnd != null; pnd = pnd.getParentNode()) {
			if (pnd instanceof Slice) {
				sliceWasFound = true;
				break;
			}
		}
		if (!sliceWasFound) {
			super.optimizeJoinOrder(node, joinArgs);
			return;
		}
		
		log.info("using Top-K");
		
		TopKEstimatorVisitor cvis = new TopKEstimatorVisitor();
		List<CardinalityVisitor.CardPair> cardPairs = new ArrayList<CardinalityVisitor.CardPair>();
		
		// pin selectors
		boolean useHashJoin = false;
		boolean useBindJoin = false;
		
		for (TupleExpr te : joinArgs) {
			te.visit(cvis);
			cardPairs.add(new CardinalityVisitor.CardPair(cvis.getNode(), cvis.getDescriptor()));
			cvis.reset();
		}
		
		// sort arguments according their cards
		cardPairs.sort((cpl, cpr) -> Long.compare(cpl.nd.card, cpr.nd.card));
		
		if (log.isTraceEnabled()) {
			log.trace("", cardPairs.get(0));
		}
		//long minCard = cardPairs.get(0).nd.card;
		//long maxCard = cardPairs.get(cardPairs.size() - 1).nd.card;
		
		CardinalityVisitor.CardPair leftArg = cardPairs.get(0);
		//result.add(cardPairs.get(0).expr);
		cardPairs.remove(0); // I expect it isn't too expensive, list is not very long (to do: try linked list)
		
		Set<String> joinVars = new HashSet<String>();
		joinVars.addAll(OptimizerUtil.getFreeVars(leftArg.expr));
		
		// look for best bound pattern
		while (!cardPairs.isEmpty()) {
			int rightIndex = 0;
			Collection<String> commonvars = null;
			for (int i = 0, n = cardPairs.size(); i < n; ++i) {
				TupleExpr arg = cardPairs.get(i).expr;
				commonvars = TopKEstimatorVisitor.getCommonVars(joinVars, arg);
				if (commonvars == null || commonvars.isEmpty()) continue;
				rightIndex = i;
				break;
			}
			
			CardinalityVisitor.CardPair rightArg = cardPairs.get(rightIndex);
			cardPairs.remove(rightIndex);
			joinVars.addAll(OptimizerUtil.getFreeVars(rightArg.expr));
			
			if (log.isTraceEnabled()) {
				log.trace("", rightArg);
			}
			
			long resultCard;
			double sel = 1;
			
			if (TopKEstimatorVisitor.ESTIMATION_TYPE == 0) {
				if (commonvars != null && !commonvars.isEmpty()) {
					resultCard = (long)Math.ceil((Math.min(leftArg.nd.card, rightArg.nd.card) / 1));
				} else {
					resultCard = (long)Math.ceil(leftArg.nd.card * rightArg.nd.card);
				}
			} else {
				if (commonvars != null && !commonvars.isEmpty()) {
					sel *= Math.min(leftArg.nd.sel, rightArg.nd.sel);
				}
				resultCard = (long)Math.ceil(leftArg.nd.card * rightArg.nd.card * sel);
			}
			
			double hashCost = rightArg.nd.card * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
			double bindCost = leftArg.nd.card / queryInfo.getFederation().getConfig().getBoundJoinBlockSize() * C_TRANSFER_QUERY + resultCard * C_TRANSFER_TUPLE;
			
			leftArg.nd.card = resultCard;
			leftArg.nd.sel = sel;
					
			if (log.isTraceEnabled()) {
				log.debug(String.format("join card: %s, hash cost: %s, bind cost: %s", resultCard, hashCost, bindCost));
			}
			
			
			NJoin newNode;
			//newNode = new BindJoin(leftArg.expr, rightArg.expr, queryInfo);
			newNode = new HashJoin(leftArg.expr, rightArg.expr, queryInfo);
			/*
			if (useHashJoin || (!useBindJoin && hashCost < bindCost)) {
				newNode = new HashJoin(leftArg.expr, rightArg.expr, queryInfo);
				//useHashJoin = true; // pin
			} else {
				newNode = new BindJoin(leftArg.expr, rightArg.expr, queryInfo);
				//useBindJoin = true; // pin
			}
			//*/
			leftArg.expr = newNode;
		}
		
		JoinRestarter head = new JoinRestarter(leftArg.expr, cvis.topksrcs, this.queryInfo);
		node.replaceWith(head);
	}
}
