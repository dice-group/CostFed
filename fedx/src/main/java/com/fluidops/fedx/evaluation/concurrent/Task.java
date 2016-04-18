package com.fluidops.fedx.evaluation.concurrent;

public abstract class Task implements Runnable, Comparable<Task> {
	int priority_;
	
	protected Task(int priority) {
		this.priority_ = priority;
	}
	
	public int getPriority() { return priority_; }
	
	public abstract void cancel();

	@Override
	public int compareTo(Task rhs) {
		return -Integer.compare(priority_, rhs.priority_);
	}
}
