package org.aksw.simba.quetsal.synopsis;

import java.io.Serializable;

public class SynopsisSlidingWindow implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3251423999126749002L;

	public Synopsis[] window;
	
	/*
	 * Synopsis containing local info, therefore is new affected by the
	 * dynamics
	 */
	private Synopsis local;
	
	private int k, t, roundsCounter;
	
	
	boolean addLocal = true;
	
	public SynopsisSlidingWindow(int k, int t){
		
		this.k = k;
		this.t = t;
		this.roundsCounter = 0;
		
		window = new Synopsis[this.k];
		
		for(int i=0;i<window.length;i++) {
			window[i] = null;
		}

		if (addLocal) add(local);
	}
	
	public SynopsisSlidingWindow(Synopsis s, int k, int t){
		
		this(k,t);
		
		this.local = s;
		
	}
	
	public Synopsis getNewest(){
		
		int i=0;
		while(i<this.k && window[i]==null)
			i++;

		if(i==this.k)
			return this.local;
		else
			return window[i];
		
		
	}
	
	public void add(Synopsis s){
		
		if(window[0] == null)
			window[0] = s;
		else
			window[0] = window[0].union(s);
		
	}
	
	/**
	 * TODO Check if union is changing local synopsis
	 * @return
	 */
	public Synopsis union(){
		
		Synopsis union = local;
		
		for(int i=0;i<window.length;i++)
				if(window[i]!=null) union = union.union(window[i]);
		
		return union;
	}
	
	public long estimatedSizeOfUnion(){
		Synopsis union = this.union();
		return union.getEstimatedSize();
	}
	
	
	public void nextRound(){
		//System.out.println("before nextround: size="+estimatedSizeOfUnion());
		
		roundsCounter++;
		
		if(roundsCounter>=t){
			
			//System.out.println("drop oldest synopses");
			/*
			 * removes oldest synopsis by shifting all synopsis
			 * and sets the one at position zero to null
			 */ 
			for(int i=(window.length-1); i>0;i--)
				window[i] = window[i-1];
			
			window[0] = null;
			
			roundsCounter=0;
			
		}
		
		if (addLocal)  add(local);
		
		//System.out.println("after nextround: size="+estimatedSizeOfUnion());
		
	}
	
}
