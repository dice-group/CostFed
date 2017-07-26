package com.fluidops.fedx.evaluation.iterator;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.rdf4j.query.QueryEvaluationException;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

public abstract class RestartableCloseableIteration<E> implements CloseableIteration<E, QueryEvaluationException> {
	
	/**
	 * Flag indicating whether this iteration has been closed.
	 */
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	/**
	 * Checks whether this CloseableIteration has been closed.
	 * 
	 * @return <tt>true</tt> if the CloseableIteration has been closed,
	 *         <tt>false</tt> otherwise.
	 */
	public final boolean isClosed() {
		return closed.get();
	}
	
	/**
	 * Calls {@link #handleClose()} upon first call and makes sure this method
	 * gets called only once.
	 */
	@Override
	public final void close()
	{
		if (closed.compareAndSet(false, true)) {
			handleClose();
		}
	}
	
	/**
	 * Calls {@link #handleRestart()} upon first call and makes sure this method
	 * gets called only once.
	 */
	public void restart()
	{
		if (closed.compareAndSet(true, false)) {
			handleRestart();
		}
	}
	
	/**
	 * Called by {@link #close} when it is called for the first time. By default, this method does
	 * nothing.
	 * 
	 */
	protected void handleClose()
	{
	}
	
	/**
	 * Called by {@link #restart} when it is called for the first time. By default, this method does
	 * nothing.
	 * 
	 */
	protected void handleRestart()
	{
	}
}
