package org.aksw.simba.quetsal.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.simba.quetsal.core.algebra.BindJoin;
import org.aksw.simba.quetsal.core.algebra.HashJoin;
import org.apache.log4j.Logger;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.optimizer.OptimizerUtil;
import com.fluidops.fedx.optimizer.StatementGroupOptimizer;
import com.fluidops.fedx.structures.QueryInfo;

public class JoinOrderOptimizer extends StatementGroupOptimizer {
	public static Logger log = Logger.getLogger(JoinOrderOptimizer.class);

    protected static double C_TRANSFER_TUPLE = 0.02;
    protected static double C_TRANSFER_QUERY = 50;
        

	Map<QueryModelNode, CardinalityVisitor.NodeDescriptor> ds = new HashMap<QueryModelNode, CardinalityVisitor.NodeDescriptor>();
	
	public class EstimatorVisitor extends CardinalityVisitor
	{
		@Override
		public void meet(StatementPattern stmt) {
			List<StatementSource> stmtSrces = queryInfo.getSourceSelection().getStmtToSources().get(stmt);
			current.card = Cardinality.getTriplePatternCardinality(stmt, stmtSrces);
			assert(current.card != 0);
			current.sel = current.card/(double)Cardinality.getTotalTripleCount(stmtSrces);
			current.mvsbjkoef = Cardinality.getTriplePatternSubjectMVKoef(stmt, stmtSrces);
			current.mvobjkoef = Cardinality.getTriplePatternObjectMVKoef(stmt, stmtSrces);
		}
		
		@Override
		public void meet(Union union) {
			union.getLeftArg().visit(this);
			NodeDescriptor temp = current;
			reset();
			union.getRightArg().visit(this);
			if (temp.card == Long.MAX_VALUE || current.card == Long.MAX_VALUE) {
				current.card = Long.MAX_VALUE;
			} else {
				current.card += temp.card;
			}
			current.sel = Math.min(current.sel, temp.sel);
		}
		
		public void meet(NJoin nj)  {
			throw new Error("NJoins must be removed");
		}
		
		@Override
		protected void meetNode(QueryModelNode node) {
			if (node instanceof StatementPattern) {
				meet((StatementPattern)node);
			} else if (node instanceof Filter) {
				meet((Filter)node);
			} else if (node instanceof Union) {
				meet((Union)node);
			} else if (node instanceof ExclusiveGroup) {
				meet((ExclusiveGroup)node);
			} else if (node instanceof NJoin) {
				meet((NJoin)node);
			} else {
				super.meetNode(node);
			}
		}
	}
	

	
	public JoinOrderOptimizer(QueryInfo queryInfo) {
		super(queryInfo);
	}
	
	/*
	@Override
	public void checkExclusiveGroup(List<ExclusiveStatement> exclusiveGroupStatements) {
		CardinalityVisitor cvis = new CardinalityVisitor();
		List<CardPair> cardPairs = new ArrayList<CardPair>();
		List<ExclusiveStatement> copy = new ArrayList<ExclusiveStatement>(exclusiveGroupStatements);
		for (ExclusiveStatement es : copy) {
			es.visit(cvis);
			cardPairs.add(new CardPair(es, cvis.getCardinality()));
			cvis.reset();
			if ("http://www.w3.org/2002/07/owl#sameAs".equals(es.getPredicateVar().getValue().toString())) {
				exclusiveGroupStatements.remove(es);
			}
		}
		// sort arguments according their cards
		cardPairs.sort((cpl, cpr) -> cpl.card.compareTo(cpr.card));
		
		for (CardPair cp : cardPairs) {
			log.trace(cp);
		}
	}
	*/
	
	@Override
	public void optimizeJoinOrder(NJoin node, List<TupleExpr> joinArgs) {
		EstimatorVisitor cvis = new EstimatorVisitor();
		List<CardinalityVisitor.CardPair> cardPairs = new ArrayList<CardinalityVisitor.CardPair>();

		// pin selectors
		boolean useHashJoin = false;
		boolean useBindJoin = false;
		
		// find card for arguments
		for (TupleExpr te : joinArgs) {
			te.visit(cvis);
			cardPairs.add(new CardinalityVisitor.CardPair(te, cvis.getDescriptor()));
			cvis.reset();
		}
		
		// sort arguments according their cards
		cardPairs.sort(new Comparator<CardinalityVisitor.CardPair>() {
			@Override
			public int compare(CardinalityVisitor.CardPair cpl, CardinalityVisitor.CardPair cpr) {
				/*
				if (cpl.expr instanceof ExclusiveGroup) {
					if (cpr.expr instanceof ExclusiveGroup) {
						return Long.compare(cpl.nd.card, cpr.nd.card);		
					} else {
						return -1;
					}
				} else if (cpr.expr instanceof ExclusiveGroup) {
					return 1;
				} else {
					return Long.compare(cpl.nd.card, cpr.nd.card);
				}
				*/
				return Long.compare(cpl.nd.card, cpr.nd.card);
			}
		});

		
		if (log.isTraceEnabled()) {
			log.trace(cardPairs.get(0));
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
				commonvars = CardinalityVisitor.getCommonVars(joinVars, arg);
				if (commonvars == null || commonvars.isEmpty()) continue;
				rightIndex = i;
				break;
			}
			
			CardinalityVisitor.CardPair rightArg = cardPairs.get(rightIndex);
			cardPairs.remove(rightIndex);
			joinVars.addAll(OptimizerUtil.getFreeVars(rightArg.expr));
			
			if (log.isTraceEnabled()) {
				log.trace(rightArg);
			}
			
			CardinalityVisitor.NodeDescriptor rd = CardinalityVisitor.getJoinCardinality(commonvars, leftArg, rightArg);

			double hashCost = rightArg.nd.card * C_TRANSFER_TUPLE + 2 * C_TRANSFER_QUERY;
			double bindCost = leftArg.nd.card / Config.getConfig().getBoundJoinBlockSize() * C_TRANSFER_QUERY + rd.card * C_TRANSFER_TUPLE;
			
			if (log.isTraceEnabled()) {
				log.debug(String.format("join card: %s, hash cost: %s, bind cost: %s", rd.card, hashCost, bindCost));
			}
			
			NJoin newNode;
			//newNode = new HashJoin(leftArg.expr, rightArg.expr, queryInfo);
			//newNode = new BindJoin(leftArg.expr, rightArg.expr, queryInfo);
			///*
			if (useHashJoin || (!useBindJoin && hashCost < bindCost && leftArg.nd.card < 1000000 && rightArg.nd.card < 1000000 )) {
				newNode = new HashJoin(leftArg.expr, rightArg.expr, queryInfo);
				//useHashJoin = true; // pin
			} else {
				newNode = new BindJoin(leftArg.expr, rightArg.expr, queryInfo);
				//useBindJoin = true; // pin
			}
			//*/
			leftArg.expr = newNode;
			leftArg.nd = rd;
		}
		node.replaceWith(leftArg.expr);
	}
}
