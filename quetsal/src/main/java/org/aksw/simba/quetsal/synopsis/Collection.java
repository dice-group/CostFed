package org.aksw.simba.quetsal.synopsis;

/*
 * Created on 16.05.2005 by Sebastian Michel
 * Max-Planck Institute for Computer Science
 * 
 * smichel@mpi-sb.mpg.de
 *
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;




/**
 * 
 * 
 *
 * @author Sebastian Michel, MPII, smichel@mpi-sb.mpg.de
 *
 */
public abstract class Collection {

    public abstract long getDocByRank(int rank);
    public abstract int size();

    @SuppressWarnings("unchecked")
	public long overlap(Collection c) {
        int count=0;
        HashMap map = new HashMap();
        for (int i=0; i<this.size(); i++) {
            map.put(new Long(getDocByRank(i)),"");
        }
        for (int i=0; i<c.size(); i++) {
            if (map.get(new Long(c.getDocByRank(i)))!=null) count++;
        }
       // System.out.println("real overlap = "+count);
        return count;
    }

    @SuppressWarnings("unchecked")
    public Collection intersection(Collection c) {
        
        HashMap map = new HashMap();
        for (int i=0; i<this.size(); i++) {
            map.put(new Long(getDocByRank(i)),"");
        }
        List in = new ArrayList();
        for (int i=0; i<c.size(); i++) {
            if (map.get(new Long(c.getDocByRank(i)))!=null) 
                in.add(new Long(c.getDocByRank(i)));
        }
        ArrayList<Long> ret = new ArrayList<Long>() ;
        for (int i=0; i<in.size(); i++) {
            ret.add(((Long) in.get(i)).longValue());
        } 
        return new ArrayCollection(ret);
    }
    

    @SuppressWarnings("unchecked")
	public Collection union(Collection c) {

        
        Set s = new HashSet();
        for (int i=0; i<this.size(); i++) {
            s.add(new Long(getDocByRank(i)));
        }

        for (int i=0; i<c.size(); i++) {
                s.add(new Long(c.getDocByRank(i)));

        }
        ArrayList<Long> ret = new ArrayList<Long>();
        int index=0;
        for (Iterator iter=s.iterator();iter.hasNext();)
            ret.add(((Long) iter.next()).longValue());
      
        return new ArrayCollection(ret);
    }

    @SuppressWarnings("unchecked")
	public long totaldistinct(Collection c) {
        HashMap map = new HashMap();
        for (int i=0; i<this.size(); i++) {
            map.put(new Long(getDocByRank(i)),"");
        }
        for (int i=0; i<c.size(); i++) {
            map.put(new Long(c.getDocByRank(i)),"");
        }
      //  System.out.println("real total distinct = "+map.keySet().size());
        return map.keySet().size();
    }

    public double resemblance(Collection c) {
        return overlap(c) / (double) totaldistinct(c);
    }

    @SuppressWarnings("unchecked")
	public long novelty(Collection c) {
        int count=0;
        HashMap map = new HashMap();
        for (int i=0; i<this.size(); i++) {
       //     System.out.println(""+(new Long(getDocByRank(i)).longValue()));
            map.put(new Long(getDocByRank(i)),"");
        }
        for (int i=0; i<c.size(); i++) {
            if (map.get(new Long(c.getDocByRank(i)))==null) {
              //  System.out.println("found new document");
                count++;
            }
        }
        return count;
    }
}
