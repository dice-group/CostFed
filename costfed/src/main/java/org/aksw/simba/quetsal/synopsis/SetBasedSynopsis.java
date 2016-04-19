package org.aksw.simba.quetsal.synopsis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.aksw.simba.quetsal.synopsis.ArrayCollection;
import org.aksw.simba.quetsal.synopsis.Collection;

public class SetBasedSynopsis extends Synopsis{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2552617001239032732L;
	Set<Long> set = new HashSet<Long>();
	
	public SetBasedSynopsis(Collection c) {
		for (int i=0; i<c.size(); i++) {
			set.add(c.getDocByRank(i));
		}
	}

	@Override
	public long getEstimatedSize() {
		return set.size();
	}

	@Override
	public long getOriginalSize() {
		return set.size();
	}

	@Override
	public Synopsis intersect(Synopsis s) {
		throw new UnsupportedOperationException("intersect in SetBasedSynopsis");
	}

	@Override
	public double noveltyWithSynopsis(Synopsis s) {
		throw new UnsupportedOperationException("noveltyWithSynopsis in SetBasedSynopsis");
	}

	@Override
	public double resemblance(Synopsis s) {
		throw new UnsupportedOperationException("resemblance in SetBasedSynopsis");
	}

	@Override
	public Synopsis union(Synopsis s) {
		SetBasedSynopsis sbs = (SetBasedSynopsis) s;
		Set<Long> se = new HashSet<Long>();
		se.addAll(this.set);
		se.addAll(sbs.set);
		
		ArrayList<Long> la = new ArrayList<Long>();
		Iterator<Long> iter = se.iterator();
		int z=0;
		while (iter.hasNext()) {
			la.add(iter.next().longValue());
			z++;
		}
		
		return new SetBasedSynopsis(new ArrayCollection(la));
		
	}

	@Override
	public int synopsisSizeInBytes() {
		// TODO Not implemented
		return 0;
	}

}

