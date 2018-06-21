package com.fluidops.fedx.structures;

public class Pair<FirstT, SecondT> {
	FirstT first;
	SecondT second;
	
	public Pair(FirstT first, SecondT second) {
		this.first = first;
		this.second = second;
	}

	public FirstT getFirst() {
		return first;
	}
	
	public SecondT getSecond() {
		return second;
	}

	@Override
	public String toString() {
		return "Pair [first=" + first + ", second=" + second + "]";
	}
}
