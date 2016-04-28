package org.aksw.simba.quetsal.core.algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.aksw.simba.quetsal.core.Cardinality;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.AbstractQueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.StatementPattern;

import com.fluidops.fedx.algebra.LocalVarsNode;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.evaluation.iterator.QueueIteration;
import com.fluidops.fedx.structures.QueryInfo;

import info.aduna.iteration.CloseableIteration;

public class TopKSourceStatementPattern extends StatementSourcePattern {
	private static final long serialVersionUID = -3691476813071027998L;

	List<Entry> srcEntries = new ArrayList<Entry>();
	
	private List<List<List<BindingSet>>> bindingCache = new ArrayList<List<List<BindingSet>>>(); // list of binding lists per src level
	private long cachedBindingCount = 0;
	
	private QueueIteration<BindingSet> iteration;
	
	public static class Entry extends AbstractQueryModelNode {
		private static final long serialVersionUID = -6553287545754379984L;
		
		public long card;
		public StatementSource src;
		public int order;
		
		public Entry(long card, StatementSource src) {
			this.card = card;
			this.src = src;
		}

		@Override
		public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
			visitor.meetOther(this);
		}
		
		@Override
		public String getSignature()
		{
			StringBuilder sb = new StringBuilder(64);
			sb.append("order: ").append(order);
			sb.append(", card: ").append(card);
			sb.append(", src: ").append(src);
			return sb.toString().trim();
		}
	}
	
	public TopKSourceStatementPattern(StatementSourcePattern arg, QueryInfo queryInfo) {
		super(arg, queryInfo);
		for (StatementSource src : arg.getStatementSources()) {
			long card = Cardinality.getTriplePatternCardinality(arg, Arrays.asList(src));
			srcEntries.add(new Entry(card,	src));
		}
		srcEntries.sort((cpl, cpr) -> -Long.compare(cpl.card, cpr.card)); // desc order
		srcEntries.get(0).order = 0;
		for (int i = 1; i < srcEntries.size(); ++i) {
			srcEntries.get(i).order = Integer.MAX_VALUE;
		}
		queryInfo.numSources.incrementAndGet();
		queryInfo.totalSources.addAndGet(srcEntries.size());
	}

	public List<Entry> getEntries() {
		return srcEntries;
	}
	
	public boolean hasEntry(Entry e) {
		for (Entry se : srcEntries) {
			if (se == e) return true;
		}
		return false;
	}
	
	public long getCardinality(int idx) {
		return srcEntries.get(idx).card;
	}
	
	public long getCardinality() {
		long result = 0;
		for (int i = 0; i < srcEntries.size(); ++i) {
			int order = srcEntries.get(i).order;
			if (order > getQueryInfo().progress) continue;
			result += srcEntries.get(i).card;
		}
		return result;
	}
	
	public List<List<List<BindingSet>>> getBindingCache() {
		return bindingCache;
	}
	
	public long getCachedBindingCount() {
		return cachedBindingCount;
	}
	
	void setIterator(QueueIteration<BindingSet> iteration) {
		this.iteration = iteration;
	}
	
	@Override
	public List<StatementSource> getStatementSources() {
		List<StatementSource> result = new ArrayList<StatementSource>();
		for (int i = 0; i < srcEntries.size(); ++i) {
			int order = srcEntries.get(i).order;
			if (order > getQueryInfo().progress) break;
			result.add(srcEntries.get(i).src);
		}
		return result;
	}
	
	@Override
	protected <X extends Exception> void visitSources(QueryModelVisitor<X> visitor)  throws X {
		for (Entry s : srcEntries) {
			s.visit(visitor);
		}
	}
	
	private void replayCache() {
		if (iteration == null) {
			iteration = new QueueIteration<BindingSet>();
		} else if (iteration.isClosed()) {
			iteration.restart();
		}
		List<StatementSource> srcs = getStatementSources();
		List<StatementSource> replaySrcs = new ArrayList<StatementSource>();
		for (int i = 0; i < srcs.size(); ++i) {
			if (bindingCache.size() <= i) {
				replaySrcs.add(srcs.get(i));
				bindingCache.add(new ArrayList<List<BindingSet>>());
			}
		}
		
		if (!replaySrcs.isEmpty()) {
			for (List<List<BindingSet>> levelcache : bindingCache) {
				for (List<BindingSet> item : levelcache) {
					iteration.executeTask(new Callable<CloseableIteration<BindingSet,QueryEvaluationException>>() {
						@Override
						public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
							return evaluate(item, replaySrcs);
						}
					});
				}
			}
		}
	}
	
	@Override
	public synchronized CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet binding) {
		replayCache();
		if (binding != null) {
			bindingCache.get(bindingCache.size() - 1).add(Arrays.asList(binding));
			cachedBindingCount++;
			iteration.executeTask(new Callable<CloseableIteration<BindingSet,QueryEvaluationException>>() {
				@Override
				public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
					return TopKSourceStatementPattern.super.evaluate(binding);
				}
			});
		}
		return iteration;
	}
	
	@Override
	public synchronized CloseableIteration<BindingSet, QueryEvaluationException> evaluate(List<BindingSet> bindings) {
		replayCache();
		if (bindings != null) {
			bindingCache.get(bindingCache.size() - 1).add(bindings);
			cachedBindingCount++;// += bindings.size();
			iteration.executeTask(new Callable<CloseableIteration<BindingSet,QueryEvaluationException>>() {
				@Override
				public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
					return TopKSourceStatementPattern.super.evaluate(bindings);
				}
			});
		}
		return iteration;
	}
}
