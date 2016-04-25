package org.aksw.simba.quetsal.core.evaluation;

import java.util.List;

import org.aksw.simba.quetsal.core.Cardinality;
import org.aksw.simba.quetsal.core.CardinalityVisitor;
import org.aksw.simba.quetsal.core.algebra.BindJoin;
import org.aksw.simba.quetsal.core.algebra.HashJoin;
import org.aksw.simba.quetsal.core.algebra.JoinRestarter;
import org.aksw.simba.quetsal.core.algebra.TopKSourceStatementPattern;
import org.aksw.simba.quetsal.core.algebra.TopKSourceStatementPattern.Entry;
import org.apache.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.evaluation.iterator.RestartableCloseableIteration;

import info.aduna.iteration.LookAheadIteration;

public class RestarterIteration<E> extends LookAheadIteration<E, QueryEvaluationException> {
	private static Logger log = Logger.getLogger(RestarterIteration.class);
	
	final JoinRestarter jr;
	RestartableCloseableIteration<E> arg;
	long resultCount = 0;
	CostVisitor visitor = new CostVisitor();
	
	static double rCost = 50; // transfer query cost
	static double tCost = 0.02; // transfer tuple cost
	
	public RestarterIteration(RestartableCloseableIteration<E> arg, JoinRestarter jr) {
		this.arg = arg;
		this.jr = jr;
	}
	
	class CVisitor extends CardinalityVisitor
	{
		@Override
		public void meet(StatementPattern stmt) {
			List<StatementSource> stmtSrces = jr.getQueryInfo().getSourceSelection().getStmtToSources().get(stmt);
			getDescriptor().card = Cardinality.getTriplePatternCardinality(stmt, stmtSrces);
		}
		
		public void meet(TopKSourceStatementPattern stmt) {
			getDescriptor().card = stmt.getCardinality();
		}
		
		public void meetJoin(TupleExpr leftArg, TupleExpr rightArg) {
			meetNode(leftArg);
			NodeDescriptor leftval = getDescriptor();
			reset();
			meetNode(rightArg);
			if (leftval.card < getCardinality()) { // card = min(leftcard, rightcard)
				setDescriptor(leftval);
			}
		}
		
		@Override
		protected void meetNode(QueryModelNode node) {
			if (node instanceof BindJoin) {
				BindJoin bj = (BindJoin)node;
				meetJoin(bj.getLeftArg(), bj.getRightArg());
			} else if (node instanceof HashJoin) {
				HashJoin bj = (HashJoin)node;
				meetJoin(bj.getLeftArg(), bj.getRightArg());
			} else if (node instanceof ExclusiveGroup) {
				meet((ExclusiveGroup)node);
			} else if (node instanceof TopKSourceStatementPattern) {
				meet((TopKSourceStatementPattern)node);
			} else if (node instanceof StatementPattern) {
				meet((StatementPattern)node);
			} else {
				throw new Error("Not implemented");
			}
		}
	}
	
	class CostVisitor extends AbstractQueryModelVisitor<RuntimeException>
	{
		long additionalResults;
		double cost;
		Entry curEntry;
		double nBindingsCfg;
		
		void reset(Entry e) {
			cost = 0;
			curEntry = e;
			nBindingsCfg = Config.getConfig().getBoundJoinBlockSize();
		}
		
		public void meet(TopKSourceStatementPattern sp)  {
			if (!sp.hasEntry(curEntry)) {
				cost = 0;
				throw new Error("logic error");
			}
			additionalResults = curEntry.card;
			cost = rCost + sp.getCardinality() * tCost;
		}
		
		public void meet(BindJoin bj)  {
			TupleExpr rightArg = bj.getRightArg();
			if (rightArg instanceof TopKSourceStatementPattern) {
				TopKSourceStatementPattern r = (TopKSourceStatementPattern)rightArg;
				if (r.hasEntry(curEntry)) {
					long modL = r.getCachedBindingCount(); // |L|/B
					cost = rCost * modL;
					// additional results min(|L|, curEntry.card)
					additionalResults = Math.min((long)(modL * nBindingsCfg), curEntry.card);
					//CVisitor cv = new CVisitor();
					//bj.getLeftArg().visit(cv);
					//additionalResults = Math.min(modL * nBindingsCfg /*cv.getCardinality()*/, curEntry.card);
					return;
				}
			}
			meetNode(bj.getLeftArg());
			cost += additionalResults/nBindingsCfg * rCost;
			CVisitor cv = new CVisitor();
			rightArg.visit(cv);
			additionalResults = Math.min(additionalResults, cv.getCardinality());
		}
		
		public void meet(HashJoin bj)  {
			TupleExpr rightArg = bj.getRightArg();
			if (rightArg instanceof TopKSourceStatementPattern) {
				TopKSourceStatementPattern r = (TopKSourceStatementPattern)rightArg;
				if (r.hasEntry(curEntry)) {
					cost = rCost + curEntry.card * tCost;
					CVisitor cv = new CVisitor();
					bj.getLeftArg().visit(cv);
					additionalResults = Math.min(cv.getCardinality(), curEntry.card); // todo: get card from internal table of Hashjoin
					return;
				}
			}
			meetNode(bj.getLeftArg());
			// cost += additionalResults * jcost
			CVisitor cv = new CVisitor();
			rightArg.visit(cv);
			additionalResults = Math.min(additionalResults, cv.getCardinality()); // todo: get card from internal table of Hashjoin
		}
		
		@Override
		protected void meetNode(QueryModelNode node) {
			if (node instanceof BindJoin) {
				meet((BindJoin)node);
			} else if (node instanceof HashJoin) {
				meet((HashJoin)node);
			} else if (node instanceof TopKSourceStatementPattern) {
				meet((TopKSourceStatementPattern)node);
			} else {
				throw new Error("CostVisitor for " + node + " is not implemented");
			}
		}
	}
	
	@Override
	protected E getNextElement() {
		while (true) {
			if (arg.hasNext()) {
				++resultCount;
				return arg.next();
			}
			// find next
			jr.getQueryInfo().progress++;
			List<Entry> entries = jr.getEntries();
			
			// find best ratio algorithm
			int maxRatioV = -1;
			double maxRatio = 0;
			for (int v = 0; v < entries.size(); ++v)
			{
				Entry e = entries.get(v);
				if (e.order != Integer.MAX_VALUE) continue;
				e.order = jr.getQueryInfo().progress;
				// find cost, card
				visitor.reset(e);
				jr.getArg(0).visit(visitor);
				double ratio = visitor.additionalResults / visitor.cost;
				if (ratio > maxRatio) {
					maxRatio = ratio;
					maxRatioV = v;
				}
				//
				e.order = Integer.MAX_VALUE;
			}

			if (maxRatioV == -1) break;
			entries.get(maxRatioV).order = jr.getQueryInfo().progress;
			log.info("got results: " + resultCount + ", Tree part: \n" + jr);
			arg.restart();
		}
		return null;
	}
}
