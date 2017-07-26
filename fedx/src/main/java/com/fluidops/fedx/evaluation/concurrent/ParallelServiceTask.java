package com.fluidops.fedx.evaluation.concurrent;

import java.util.concurrent.Callable;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

public class ParallelServiceTask implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {
	
	protected final FedXService service;
	protected final FederationEvalStrategy strategy;
	protected final BindingSet bindings;
	
	public ParallelServiceTask(FedXService service, FederationEvalStrategy strategy, BindingSet bindings) {
		this.service = service;
		this.strategy = strategy;
		this.bindings = bindings;
	}
	
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> call() {
		return strategy.evaluate(service.getService(), bindings);	
	}

}
