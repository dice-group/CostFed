package com.fluidops.fedx.evaluation.concurrent;

import java.util.concurrent.Callable;

import com.fluidops.fedx.structures.QueryInfo;

public abstract class Async<T> extends Task {
	Callable<T> cl_;
	
	protected Async(Callable<T> cl) {
		super(QueryInfo.getPriority() + 1);
		cl_ = cl;
	}
	
	@Override
	public void run() {
		QueryInfo.setPriority(getPriority());
		T result;
		try {
			result = cl_.call();
		} catch (Exception e) {
			exception(e);
			return;
		}
		callAsync(result);
	}

	@Override
	public void cancel() {
		exception(new InterruptedException("cancelled"));
	}
	
	public abstract void callAsync(T val);
	public abstract void exception(Exception val);
}
