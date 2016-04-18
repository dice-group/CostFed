package org.aksw.simba.quetsal.synopsis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class PoolCollection {
	
	int size;
	Random random;
	Set<Long> pool;
	
	public PoolCollection(int size, int seed) {
		this.size = size;
		this.random = new Random(seed);
		//first no multisets
		pool = new HashSet<Long>();
		for (int i=0; i<size; i++) {
			pool.add((long) i);
		}
	}
	
	
	public Set<Long> getRandomSubset(int s) {
		Set<Long> ret = new HashSet<Long>(s);
		List<Long> current = new ArrayList<Long>();
		current.addAll(pool);
		while (ret.size()<s) {
			int pos = random.nextInt(current.size());
			long l = current.get(pos);
			ret.add(l);
			current.remove(pos);
		}
		return ret;
	}
	
	

}

