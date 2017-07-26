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

package com.fluidops.fedx.evaluation.join;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.concurrent.Async;
import com.fluidops.fedx.evaluation.iterator.BufferedCloseableIterator;
import com.fluidops.fedx.evaluation.iterator.QueueIterator;
import com.fluidops.fedx.structures.QueryInfo;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;


/**
 * Base class for any join parallel join executor. 
 * 
 * Note that this class extends {@link LookAheadIteration} and thus any implementation of this 
 * class is applicable for pipelining when used in a different thread (access to shared
 * variables is synchronized).
 * 
 * @author Andreas Schwarte
 */
public abstract class JoinExecutorBase<T> extends LookAheadIteration<T, QueryEvaluationException> {

	static Logger log = LoggerFactory.getLogger(JoinExecutorBase.class);
	
	protected static int NEXT_JOIN_ID = 1;
	
	/* Constants */
	protected final FederationEvalStrategy strategy;		// the evaluation strategy
	protected final TupleExpr rightArg;						// the right argument for the join
	protected final BindingSet bindings;					// the bindings
	protected final int joinId;								// the join id
	protected final QueryInfo queryInfo;
	
	/* Variables */
	protected volatile Thread evaluationThread;
	protected CloseableIteration<T, QueryEvaluationException> leftIter;
	protected CloseableIteration<T, QueryEvaluationException> rightIter;
	protected volatile boolean closed;
	protected boolean finished = false;
	
	//protected QueueCursor<CloseableIteration<T, QueryEvaluationException>> rightQueue = new QueueCursor<CloseableIteration<T, QueryEvaluationException>>(1024);
	private QueueIterator<CloseableIteration<T, QueryEvaluationException>> rightQueue =
			new QueueIterator<CloseableIteration<T, QueryEvaluationException>>(new QueueIterator.ItemReleaser<CloseableIteration<T, QueryEvaluationException>>() {
				@Override
				public void release(CloseableIteration<T, QueryEvaluationException> item) {
					item.close();
				}
			});
	
	public JoinExecutorBase(FederationEvalStrategy strategy, CloseableIteration<T, QueryEvaluationException> leftIter, TupleExpr rightArg,
			BindingSet bindings, QueryInfo queryInfo)
	{
		this.strategy = strategy;
		this.leftIter = leftIter;
		this.rightArg = rightArg;
		this.bindings = bindings;
		this.joinId = NEXT_JOIN_ID++;
		this.queryInfo = queryInfo;
	}
	
	/**
	 * Implementations must implement this method to handle bindings.
	 * 
	 * Use the following as a template
	 * <code>
	 * while (!closed && leftIter.hasNext()) {
	 * 		// your code
	 * }
	 * </code>
	 * 
	 * and add results to rightQueue. Note that addResult() is implemented synchronized
	 * and thus thread safe. In case you can guarantee sequential access, it is also
	 * possible to directly access rightQueue
	 * 
	 */
	protected abstract void handleBindings();
	
	public class JoinTask extends Async<CloseableIteration<T, QueryEvaluationException>> {
		
		JoinTask(Callable<CloseableIteration<T, QueryEvaluationException>> cl) {
			super(cl);
			rightQueue.onAddIterator();
		}
		
		@Override
		public void run() {
			QueryInfo.setPriority(1);
			super.run();
		}
		
		@Override
		public void callAsync(CloseableIteration<T, QueryEvaluationException> res) {
			/* optimization: avoid adding empty results */
			if (res instanceof EmptyIteration<?,?>) {
				rightQueue.onRemoveIterator();
				return;
			}
			//log.info("join: " + res.getClass().toString());
			if (res instanceof org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult) {
				rightQueue.add_release(new BufferedCloseableIterator<T, QueryEvaluationException>(res));
			} else {
				rightQueue.add_release(res);
			}
		}

		@Override
		public void exception(Exception e) {
			log.warn("Error executing union operator: " + e.getMessage());
			rightQueue.add(e);
		}
	}
	
	public void addTask(Callable<CloseableIteration<T, QueryEvaluationException>> c) {
		doAddTask(new JoinTask(c));
	}
	
	protected abstract void doAddTask(JoinTask jt);
	
	@Override
	public T getNextElement() {
		while (rightIter != null || rightQueue.hasNext()) {
			if (rightIter == null) {
				rightIter = rightQueue.next();
			}
			if (rightIter.hasNext()) {
				return rightIter.next();
			}
			else {
				rightIter.close();
				rightIter = null;
			}
		}

		return null;
	}

	
	@Override
	public void handleClose() throws QueryEvaluationException {
		closed = true;
		if (evaluationThread != null) {
			evaluationThread.interrupt();
		}
		
		if (rightIter != null) {
			rightIter.close();
			rightIter = null;
		}

		leftIter.close();
		rightQueue.close();
	}
	
	/**
	 * Return true if this executor is finished or aborted
	 * 
	 * @return
	 */
	public boolean isFinished() {
		synchronized (this) {
			return finished;
		}
	}
	
	/**
	 * Retrieve information about this join, joinId and queryId
	 * 
	 * @return
	 */
	public String getId() {
		return "ID=(id:" + joinId + "; query:" + getQueryId() + ")";
	}
	
	//@Override
	public int getQueryId() {
		if (queryInfo!=null)
			return queryInfo.getQueryID();
		return -1;
	}
}
