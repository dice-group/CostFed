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

package com.fluidops.fedx.evaluation.concurrent;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;


/**
 * Parallel executor for {@link FedXService} nodes, which wrap SERVICE expressions.
 * 
 * Uses the union scheduler to execute the task
 * 
 * @author Andreas Schwarte
 */
public class ParallelServiceExecutor extends LookAheadIteration<BindingSet, QueryEvaluationException> implements ParallelExecutor<BindingSet> {
	
	public static Logger log = Logger.getLogger(ParallelServiceExecutor.class);
	
	protected final FedXService service;
	protected final FederationEvalStrategy strategy;
	protected final BindingSet bindings;
	
	protected CloseableIteration<BindingSet, QueryEvaluationException> rightIter = null;
	protected boolean finished = false;
	protected Exception error = null;
	
	/**
	 * @param service
	 * @param strategy
	 * @param bindings
	 */
	public ParallelServiceExecutor(FedXService service,
			FederationEvalStrategy strategy, BindingSet bindings) {
		super();
		this.service = service;
		this.strategy = strategy;
		this.bindings = bindings;
	}

	
	@Override
	public void run() {
		int taskPriotity = service.getQueryInfo().getPriority() + 1;
		FederationManager.getInstance().getScheduler().schedule(new ParallelServiceTask(taskPriotity), taskPriotity);			
	}

	@Override
	public void addResult(CloseableIteration<BindingSet, QueryEvaluationException> res) {

		synchronized (this) {
			
			rightIter = res;
			this.notify();
		}
		
	}

	@Override
	public void toss(Exception e)	{

		synchronized (this) {
			error = e;
		}
		
	}

	@Override
	public void done()	{
		;	// no-op		
	}

	@Override
	public boolean isFinished()	{
		synchronized (this) {
			return finished;
		}
	}

	@Override
	public int getQueryId()	{
		return service.getQueryInfo().getQueryID();
	}

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		
		// error resulting from TOSS
		if (error!=null) {
			if (error instanceof QueryEvaluationException)
				throw (QueryEvaluationException)error;
			throw new QueryEvaluationException(error);
		}
			
		if (rightIter==null) {	
			// block if not evaluated
			synchronized (this) {
				if (rightIter==null) {
					try	{
						// wait until the service expression is evaluated
						this.wait();
					}	catch (InterruptedException e)	{
						log.debug("Interrupted exception while evaluating service.");
					}
				}
			}
		}
		
		if (rightIter.hasNext())
			return rightIter.next();		
		
		return null;
	}


	
	/**
	 * Task for evaluating service requests
	 * 
	 * @author Andreas Schwarte
	 */
	private class ParallelServiceTask implements Callable<CloseableIteration<BindingSet, QueryEvaluationException>> {
		int priority;
		ParallelServiceTask(int priority) {
			this.priority = priority;
		}
		
		@Override
		public CloseableIteration<BindingSet, QueryEvaluationException> call() throws Exception {
			service.getQueryInfo().setPriority(priority);
			return strategy.evaluate(service.getService(), bindings);
		}
	}
}
