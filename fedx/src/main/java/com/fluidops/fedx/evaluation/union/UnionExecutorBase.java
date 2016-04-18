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

package com.fluidops.fedx.evaluation.union;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;

import com.fluidops.fedx.evaluation.concurrent.Async;
import com.fluidops.fedx.evaluation.iterator.BufferedCloseableIterator;
import com.fluidops.fedx.evaluation.iterator.QueueIterator;
import com.fluidops.fedx.structures.QueryInfo;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.LookAheadIteration;


/**
 * Base class for any parallel union executor.
 * 
 * Note that this class extends {@link LookAheadIteration} and thus any implementation of this 
 * class is applicable for pipelining when used in a different thread (access to shared
 * variables is synchronized).
 * 
 * @author Andreas Schwarte
 *
 */
public abstract class UnionExecutorBase<T> extends LookAheadIteration<T, QueryEvaluationException> {

	public static Logger log = Logger.getLogger(UnionExecutorBase.class);
	protected static AtomicInteger NEXT_UNION_ID = new AtomicInteger(0);
	
	/* Constants */
	protected final int unionId;							// the union id
	
	/* Variables */
	protected volatile boolean closed;
	protected boolean finished = true;
	
	
	//protected QueueCursor<CloseableIteration<T, QueryEvaluationException>> result = new QueueCursor<CloseableIteration<T, QueryEvaluationException>>(1024);
	protected QueueIterator<CloseableIteration<T, QueryEvaluationException>> result =
			new QueueIterator<CloseableIteration<T, QueryEvaluationException>>(new QueueIterator.ItemReleaser<CloseableIteration<T, QueryEvaluationException>>() {
				@Override
				public void release(CloseableIteration<T, QueryEvaluationException> item) {
					item.close();
				}
			});
	
	protected CloseableIteration<T, QueryEvaluationException> rightIter = null;

	
	public UnionExecutorBase() {
		this.unionId = NEXT_UNION_ID.incrementAndGet();
	}
	
	public class UnionTask extends Async<CloseableIteration<T, QueryEvaluationException>> {
		UnionTask(Callable<CloseableIteration<T, QueryEvaluationException>> cl) {
			super(cl);
			result.onAddIterator();
		}
		
		@Override
		public void callAsync(CloseableIteration<T, QueryEvaluationException> res) {
			/* optimization: avoid adding empty results */
			if (res instanceof EmptyIteration<?,?>) {
				result.onRemoveIterator();
				return;
			}
		
			//log.info("union: " + res.getClass().toString());
			if (res instanceof org.openrdf.http.client.BackgroundTupleResult) {
				result.add_release(new BufferedCloseableIterator<T, QueryEvaluationException>(res));
			} else {
				result.add_release(res);
			}
		}

		@Override
		public void exception(Exception e) {
			log.warn("Error executing union operator: " + e.getMessage());
			result.add(e);
		}
	}
	
	public void addTask(Callable<CloseableIteration<T, QueryEvaluationException>> c) {
		doAddTask(new UnionTask(c));
	}
	
	protected abstract void doAddTask(UnionTask ut);
	
	@Override
	public T getNextElement() {
		while (rightIter != null || result.hasNext()) {
			if (rightIter == null) {
				rightIter = result.next();
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
	public void handleClose() {
		closed = true;
		
		if (rightIter != null) {
			rightIter.close();
			rightIter = null;
		}
		result.close();
	}
	
//	/**
//	 * Return true if this executor is finished or aborted
//	 * 
//	 * @return
//	 */
//	public boolean isFinished() {
//		synchronized (this) {
//			return finished;
//		}
//	}
}
