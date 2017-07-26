package com.fluidops.fedx.evaluation.concurrent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.exception.RuntimeInterruptedException;

public class ControlledWorkerScheduler implements Scheduler {
	private static final Logger log = LoggerFactory.getLogger(ControlledWorkerScheduler.class);
	private static final AtomicInteger threadCounter_ = new AtomicInteger(0);
	private static final long timeoutQuantMls_ = 100;
	
	int maxWorkerCount_;
	Set<WorkerThread> threads_ = new HashSet<WorkerThread>();
	BlockingQueue<WorkerThread> threadQueue_ = new LinkedBlockingQueue<WorkerThread>();
	//BlockingDeque<Runnable> taskQueue_ = new LinkedBlockingDeque<Runnable>();
	PriorityBlockingQueue<Task> taskQueue_ = new PriorityBlockingQueue<Task>();
	
	String schedulerName_;
	AtomicBoolean finished_ = new AtomicBoolean(false);
	AtomicInteger lockedThreadCount_ = new AtomicInteger(1);
	
	public ControlledWorkerScheduler(int maxWorkerCount, String name) {
		maxWorkerCount_ = maxWorkerCount;
		threadQueue_ = new LinkedBlockingQueue<WorkerThread>(maxWorkerCount);
		schedulerName_ = name;

		createNewThread();
	}
	
	private void createNewThread() {
		synchronized (threads_) {
			if (finished_.get()) return;
			WorkerThread t = new WorkerThread(new Runnable() {
				@Override
				public void run() {
					threadProc();
				}
			}, getNextThreadName());
			threads_.add(t);
			t.start();
		}
	}
	
	private void threadProc() {
		WorkerThread currentThread = (WorkerThread)Thread.currentThread();
		
		while (!finished_.get()) {
			Thread.interrupted(); // to make sure the interruption status is cleared
			Task task;
			try {
				task = taskQueue_.take();
			} catch (InterruptedException e) {
				continue;
			}
			
			// get next thread from the pool or allocate new one
			WorkerThread t = null;
			int tcount;
			synchronized (threads_) {
				tcount = threads_.size();
			}
			int k = tcount - maxWorkerCount_ - lockedThreadCount_.get();
			if (k >= 0) {
				long timeout = timeoutQuantMls_ * (long)(Math.pow(2, (double)k));
				log.debug("thread queue is running out of capacity, waiting for a free thread or allocatting new in " + timeout + " milliseconds");
				try {
					t = threadQueue_.poll(timeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ignore) {
					// do nothing
				}
			} else {
				t = threadQueue_.poll();
			}
			
			if (t != null) {
				t.awake();
			} else {
				createNewThread();
			}
			
			synchronized(taskQueue_) {
				if (finished_.get()) {
					task.cancel();
					break;
				}
			}
			
			if (log.isTraceEnabled()) {
				log.trace("start task: " + task.hashCode() + ", priority: " + task.priority_);
			}
			task.run();
			if (log.isTraceEnabled()) {
				log.trace("end task: " + task.hashCode());
			}
			
			// park or release
			try {
				synchronized (currentThread) { // to avoid thread polling before parking
					if (finished_.get()) break;
					if (threadQueue_.offer(currentThread)) {
						currentThread.nosyncPark();
					} else {
						break;
					}
				}
			} catch (InterruptedException ignore) {
				// do nothing
			}
		}
		synchronized (threads_) {
			threads_.remove(currentThread);
			threads_.notify(); // shutdown lock support
		}
	}
		
	private String getNextThreadName() {
		return schedulerName_ + "_" + (threadCounter_.incrementAndGet() - 1);
	}
	
	@Override
	public void schedule(Task task) {
		synchronized (taskQueue_) {
			if (finished_.get()) {
				task.cancel();
			} else {
				taskQueue_.put(task);
				if (log.isDebugEnabled()) {
					log.debug("put task: " + task + ", priority: " + task.getPriority() + ", queue size: " + taskQueue_.size());
				}
			}
		}
	}
	
	@Override
	public <R> Future<R> schedule(Callable<R> task, int priority) {
		FutureTask<R> f = new FutureTask<R>(task);
		schedule(new WorkerTask(f, priority));
		return f;
	}

	@Override
	public void shutdown() {
		finished_.set(true);
		synchronized (taskQueue_) {
			while (true) {
				Task t = taskQueue_.poll();
				if (null == t) {
					break;
				}
				t.cancel();
			}
		}
		
		synchronized (threads_) {
			for (Thread t : threads_) {
				WorkerThread wt = (WorkerThread)t;
				synchronized(wt) {
					wt.awake();
				}
			}
			
			// just to unlock queue
			taskQueue_.put(new Task(0) {
				@Override public void run() {}
				@Override public void cancel() {}
			});
			//try { taskQueue_.put(new Runnable() { @Override public void run() {}});	} catch (InterruptedException ignore) {}
			
			while (!threads_.isEmpty()) {
				try {
					threads_.wait();
				} catch (InterruptedException ex) {
					throw new RuntimeInterruptedException(ex);
				}
			}
		}
		log.info("Scheduler has been shutdowned.");
	}

	@Override
	public boolean isRunning() {
		return !finished_.get();
	}

	public void abort(int queryId) {
		log.debug("Aborting tasks for query with id " + queryId + ".");
		throw new Error("not implemented");
	}
	
	private static class WorkerThread extends Thread {
		boolean parked_ = false;
		
		public WorkerThread(Runnable r, String name) {
			super(r, name);
		}
		
		//public synchronized void park() throws InterruptedException {
		//	nosyncPark();
		//}
		
		public void nosyncPark() throws InterruptedException {
			parked_ = true;
			while (parked_) {
				this.wait();
			}
		}
		
		public synchronized void awake() {
			parked_ = false;
			this.notify();
		}
	}
	private static class WorkerTask extends Task {
		RunnableFuture<?> runnable_;
		
		WorkerTask(RunnableFuture<?> runnable, int priority) {
			super(priority);
			this.runnable_ = runnable;
		}
		
		@Override
		public void run() {
			runnable_.run();
		}

		@Override
		public void cancel() {
			runnable_.cancel(true);
		}
	}
}
