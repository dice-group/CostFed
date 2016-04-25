package com.fluidops.fedx.evaluation.iterator;

import java.util.NoSuchElementException;

public abstract class RestartableLookAheadIteration<E> extends RestartableCloseableIteration<E> {
	
	private E nextElement;
	
	/**
	 * Gets the next element. Subclasses should implement this method so that it
	 * returns the next element.
	 * 
	 * @return The next element, or <tt>null</tt> if no more elements are
	 *         available.
	 */
	protected abstract E getNextElement();
	
	@Override
	public final boolean hasNext()
	{
		lookAhead();

		return nextElement != null;
	}
	
	@Override
	public final E next()
	{
		lookAhead();

		E result = nextElement;

		if (result != null) {
			nextElement = null;
			return result;
		}
		else {
			throw new NoSuchElementException();
		}
	}
	
	/**
	 * Fetches the next element if it hasn't been fetched yet and stores it in
	 * {@link #nextElement}.
	 * 
	 */
	private void lookAhead()
	{
		if (nextElement == null && !isClosed()) {
			nextElement = getNextElement();

			if (nextElement == null) {
				close();
			}
		}
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected void handleClose()
	{
		super.handleClose();
		nextElement = null;
	}
}
