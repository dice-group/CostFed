package org.aksw.simba.quetsal.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.optimizer.OptimizerUtil;
import com.fluidops.fedx.optimizer.StatementGroupOptimizer;
import com.fluidops.fedx.structures.QueryInfo;

public class JoinOrderOptimizer extends StatementGroupOptimizer {
	public static Logger log = Logger.getLogger(JoinOrderOptimizer.class);

    private static double C_TRANSFER_TUPLE = 0.02;
    private static double C_TRANSFER_QUERY = 50;
    private static int ESTIMATION_TYPE = 0;
    
	class NodeDescriptor {
		long card = Long.MAX_VALUE;
		double sel = 0;
	}
	
	Map<QueryModelNode, NodeDescriptor> ds = new HashMap<QueryModelNode, NodeDescriptor>();
	
	static Collection<String> getCommonVars(Collection<String> vars, TupleExpr tupleExpr) {
		Collection<String> commonvars = null;
		Collection<String> exprVars = OptimizerUtil.getFreeVars(tupleExpr);
		for (String argvar : exprVars) {
			if (vars.contains(argvar)) {
				if (commonvars == null) {
					commonvars = new HashSet<String>();
				}
				commonvars.add(argvar);
			}
		}
		return commonvars;
	}
	
	class EstimatorVisitor extends AbstractQueryModelVisitor<RuntimeException>
	{
		NodeDescriptor current = new NodeDescriptor();
		
		void reset() {
			current = new NodeDescriptor();
		}
		
		NodeDescriptor getDescriptor() {
			return current;
		}
		
		long getCardinality() {
			return current.card;
		}
		
		@Override
		public void meet(StatementPattern stmt) {
			List<StatementSource> stmtSrces = queryInfo.getSourceSelection().getStmtToSources().get(stmt);
			current.card = Cardinality.getTriplePatternCardinality(stmt, stmtSrces);
			current.sel = current.card/(double)Cardinality.getTotalTripleCount(stmtSrces);
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
		
		@Override
		public void meet(Filter filter)  {
			filter.getArg().visit(this);
		}
		
		@SuppressWarnings("unused")
		public void meet(ExclusiveGroup eg)  {
			List<ExclusiveStatement> slst = new LinkedList<ExclusiveStatement>(eg.getStatements()); // make copy
			List<StatementSource> stmtSrces = queryInfo.getSourceSelection().getStmtToSources().get(slst.get(0));
			assert (stmtSrces.size() == 1);
			long total = Cardinality.getTotalTripleCount(stmtSrces);
			
			ExclusiveStatement leftArg = slst.get(0);
			slst.remove(0);
			
			Set<String> joinVars = new HashSet<String>();
			joinVars.addAll(OptimizerUtil.getFreeVars(leftArg));
			
			long leftCard = Cardinality.getTriplePatternCardinality(leftArg, stmtSrces);
			double leftSelectivity = leftCard/(double)total;
			
			// find possible join order
			while (!slst.isEmpty()) {
				ExclusiveStatement rightArg = null;
				Collection<String> commonvars = null;
				for (int i = 0, n = slst.size(); i < n; ++i) {
					ExclusiveStatement arg = slst.get(i);
					commonvars = getCommonVars(joinVars, arg);
					if (commonvars == null || commonvars.isEmpty()) continue;
					rightArg = arg;
					slst.remove(i);
					break;
				}
				if (rightArg == null) {
					rightArg = slst.get(0);
					slst.remove(0);
				}
				long rightCard = Cardinality.getTriplePatternCardinality(rightArg, stmtSrces);
				
				joinVars.addAll(OptimizerUtil.getFreeVars(rightArg));
				
				double sel = 1;
				
				if (ESTIMATION_TYPE == 0) {
					if (commonvars != null && !commonvars.isEmpty()) {
						leftCard = (long)Math.ceil((Math.min(leftCard, rightCard) / 2));
					} else {
						leftCard = (long)Math.ceil(leftCard * rightCard);
					}
				} else if (ESTIMATION_TYPE == 1) {
					double rightSelectivity = rightCard/(double)total;
					if (commonvars != null && !commonvars.isEmpty()) {
						sel = leftSelectivity * rightSelectivity;
					}
					leftCard = (long)Math.ceil(leftCard * rightCard * sel);
					leftSelectivity = sel;
				} else {
					double rightSelectivity = rightCard/(double)total;
					if (commonvars != null && !commonvars.isEmpty()) {
						sel *= Math.min(leftSelectivity, rightSelectivity);
					}
					leftCard = (long)Math.ceil(leftCard * rightCard * sel);
					leftSelectivity = sel;
				}
			}
			current.card = leftCard;
			current.sel = leftSelectivity;
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
	
	public class CardPair {
		public TupleExpr expr;
		public NodeDescriptor nd;
		
		public CardPair(TupleExpr te, NodeDescriptor nd) {
			this.expr = te;
			this.nd = nd;
		}

		@Override
		public String toString() {
			return String.format("CardPair [expr=%s, card=%s, sel=%s", expr, nd.card, nd.sel);
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
		List<CardPair> cardPairs = new ArrayList<CardPair>();

		// pin selectors
		boolean useHashJoin = false;
		boolean useBindJoin = false;
		
		// find card for arguments
		for (TupleExpr te : joinArgs) {
			te.visit(cvis);
			cardPairs.add(new CardPair(te, cvis.getDescriptor()));
			cvis.reset();
		}
		
		// sort arguments according their cards
		cardPairs.sort((cpl, cpr) -> Long.compare(cpl.nd.card, cpr.nd.card));
		
		if (log.isTraceEnabled()) {
			log.trace(cardPairs.get(0));
		}
		//long minCard = cardPairs.get(0).nd.card;
		//long maxCard = cardPairs.get(cardPairs.size() - 1).nd.card;
		
		CardPair leftArg = cardPairs.get(0);
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
				commonvars = getCommonVars(joinVars, arg);
				if (commonvars == null || commonvars.isEmpty()) continue;
				rightIndex = i;
				break;
			}
			
			CardPair rightArg = cardPairs.get(rightIndex);
			cardPairs.remove(rightIndex);
			joinVars.addAll(OptimizerUtil.getFreeVars(rightArg.expr));
			
			if (log.isTraceEnabled()) {
				log.trace(rightArg);
			}
			
			long resultCard;
			double sel = 1;
			
			if (ESTIMATION_TYPE == 0) {
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
			double bindCost = leftArg.nd.card / Config.getConfig().getBoundJoinBlockSize() * C_TRANSFER_QUERY + resultCard * C_TRANSFER_TUPLE;
			
			leftArg.nd.card = resultCard;
			leftArg.nd.sel = sel;
					
			if (log.isTraceEnabled()) {
				log.debug(String.format("join card: %s, hash cost: %s, bind cost: %s", resultCard, hashCost, bindCost));
			}
			
			NJoin newNode;
			if (useHashJoin || (!useBindJoin && hashCost < bindCost)) {
				newNode = new HashJoin(leftArg.expr, rightArg.expr, queryInfo);
				//useHashJoin = true; // pin
			} else {
				newNode = new BindJoin(leftArg.expr, rightArg.expr, queryInfo);
				//useBindJoin = true; // pin
			}
			leftArg.expr = newNode;
		}
		node.replaceWith(leftArg.expr);
	}
}
