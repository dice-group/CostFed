package com.fluidops.fedx.evaluation.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;

import com.fluidops.fedx.exception.RuntimeInterruptedException;

public class QueueIterator <E> extends RestartableLookAheadIteration<E> {
	static Logger log = Logger.getLogger(QueueIterator.class);
	
	private ItemReleaser<E> itemReleaser = null;
	protected Queue<E> rqueue_ = new LinkedList<E>();
	protected Queue<Exception> exceptions_ = new LinkedList<Exception>();
	protected AtomicInteger tasksCount_ = new AtomicInteger(0);
	
	// I need synchronized flag
	private boolean closed_ = false;
	
	public static interface ItemReleaser <E> {
		void release(E item);
	}
	
	public QueueIterator(ItemReleaser<E> itemReleaser) {
		this.itemReleaser = itemReleaser;
	}
	
	@Override
	protected synchronized E getNextElement() {
		while (!closed_) {
			E result = rqueue_.poll();
			if (result != null) return result;
			if (tasksCount_.get() == 0) {
				checkException();
				break;
			}
			try {
				this.wait();
			} catch (InterruptedException e) {
				throw new RuntimeInterruptedException(e);
			}
		}
		return null;
	}

	public void onAddIterator() {
		tasksCount_.incrementAndGet();
	}
	
	public void onRemoveIterator() {
		if (tasksCount_.decrementAndGet() == 0) {
			synchronized(this) {
				this.notify();
			}
		}
	}
	
	public synchronized void simple_add(E val) {
		if (!closed_) {
			rqueue_.add(val);
		}
		this.notify();
	}
	
	public synchronized void add_release(E val) {
		if (!closed_) {
			rqueue_.add(val);
			tasksCount_.decrementAndGet();
		} else {
			itemReleaser.release(val);
		}
		this.notify();
	}
	
	public synchronized void add(Exception e) {
		exceptions_.add(e);
		tasksCount_.decrementAndGet();
		this.notify();
	}
	
	public void checkException()
	{
		if (!exceptions_.isEmpty()) {
			try {
				throw exceptions_.poll();
			}
			catch (QueryEvaluationException e) {
				List<StackTraceElement> stack = new ArrayList<StackTraceElement>();
				stack.addAll(Arrays.asList(e.getStackTrace()));
				StackTraceElement[] thisStack = new Throwable().getStackTrace();
				stack.addAll(Arrays.asList(thisStack).subList(1, thisStack.length));
				e.setStackTrace(stack.toArray(new StackTraceElement[stack.size()]));
				throw e;
			}
			catch (RuntimeException e) {
				List<StackTraceElement> stack = new ArrayList<StackTraceElement>();
				stack.addAll(Arrays.asList(e.getStackTrace()));
				StackTraceElement[] thisStack = new Throwable().getStackTrace();
				stack.addAll(Arrays.asList(thisStack));
				e.setStackTrace(stack.toArray(new StackTraceElement[stack.size()]));
				throw e;
			}
			catch (Throwable e) {
				throw new QueryEvaluationException(e);
			}
		}
	}
	
	@Override
	public synchronized void handleClose() {
		if (closed_) return;
		for (E item = rqueue_.poll(); item != null; item = rqueue_.poll()) {
			try {
				itemReleaser.release(item);
			} catch (Throwable t) {
				log.error("Error while releasing items", t);
			}
		}
		closed_ = true;
	}
	
	@Override
	public synchronized void handleRestart() {
		closed_ = false;
	}
}
