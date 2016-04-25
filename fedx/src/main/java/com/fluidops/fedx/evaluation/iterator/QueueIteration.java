package com.fluidops.fedx.evaluation.iterator;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;

import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.evaluation.concurrent.Async;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

public class QueueIteration<E> extends RestartableLookAheadIteration<E> {
	static final Logger log = Logger.getLogger(QueueIteration.class);

	protected QueueIterator<CloseableIteration<E, QueryEvaluationException>> resultQueue =
		new QueueIterator<CloseableIteration<E, QueryEvaluationException>>(
			new QueueIterator.ItemReleaser<CloseableIteration<E, QueryEvaluationException>>() {
				@Override
				public void release(CloseableIteration<E, QueryEvaluationException> item) {
					item.close();
				}
			}
		);
	
	protected CloseableIteration<E, QueryEvaluationException> resultIter;
	
	public QueueIteration() {
		
	}
	
	public QueueIterator<CloseableIteration<E, QueryEvaluationException>> getResultQueue() {
		return resultQueue;
	}
	
	@Override
	protected E getNextElement() {
		while (resultIter != null || resultQueue.hasNext()) {
			if (resultIter == null) {
				resultIter = resultQueue.next();
			}
			if (resultIter.hasNext()) {
				return resultIter.next();
			} else {
				resultIter.close();
				resultIter = null;
			}
		}

		return null;
	}
	
	@Override
	public void handleRestart() {
		resultQueue.restart();
	}
	
	public class QueueTask extends Async<CloseableIteration<E, QueryEvaluationException>> {
		
		public QueueTask(Callable<CloseableIteration<E, QueryEvaluationException>> cl) {
			super(cl);
			resultQueue.onAddIterator();
		}
		
		@Override
		public void callAsync(CloseableIteration<E, QueryEvaluationException> res) {
			/* optimization: avoid adding empty results */
			if (res instanceof EmptyIteration<?,?>) {
				resultQueue.onRemoveIterator();
				return;
			}
			//log.info("queue: " + res.getClass().toString());
			if (res instanceof org.openrdf.http.client.BackgroundTupleResult) {
				resultQueue.add_release(new BufferedCloseableIterator<E, QueryEvaluationException>(res));
			} else {
				resultQueue.add_release(res);
			}
		}

		@Override
		public void exception(Exception e) {
			log.warn("Error executing queue operator: " + e.getMessage());
			resultQueue.add(e);
		}
		
		@Override
		public void cancel() {
			resultQueue.onRemoveIterator();
		}
	}
	
	public QueueTask createTask(Callable<CloseableIteration<E, QueryEvaluationException>> async) {
		return new QueueTask(async);
	}
	
	public void executeTask(Callable<CloseableIteration<E, QueryEvaluationException>> async) {
		FederationManager.getInstance().getScheduler().schedule(new QueueTask(async));
	}

}
