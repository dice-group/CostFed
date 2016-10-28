package org.aksw.simba.quetsal.core.evaluation;

import org.aksw.simba.quetsal.core.algebra.TopKSourceStatementPattern;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.fluidops.fedx.evaluation.iterator.RestartableLookAheadIteration;

import info.aduna.iteration.CloseableIteration;

public class TopKSourceStatementIteration extends RestartableLookAheadIteration<BindingSet>{

	final TopKSourceStatementPattern tkssp;
	CloseableIteration<BindingSet, QueryEvaluationException> current_;
	
	public TopKSourceStatementIteration(TopKSourceStatementPattern tkssp, BindingSet binding) {
		this.tkssp = tkssp;
		current_ = tkssp.evaluate(binding);
	}
	
	@Override
	protected BindingSet getNextElement() {
		if (current_ == null) {
			current_ = tkssp.evaluate((BindingSet)null); // binding is stored in tkssp cache and replayed for next sources
		}
		if (current_.hasNext()) {
			return current_.next();
		}

		return null;
	}
	
	@Override
	public void handleRestart() {
		current_ = null;
	}
}
