package com.fluidops.fedx.exception;

public class RuntimeInterruptedException extends RuntimeException {
	private static final long serialVersionUID = -7462580466776607973L;
	public RuntimeInterruptedException(InterruptedException e) {
		super(e);
	}
}
