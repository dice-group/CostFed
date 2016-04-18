/*
 * Created on 15.06.2005 by Sebastian Michel
 * Max-Planck Institute for Computer Science
 * 
 * smichel@mpi-sb.mpg.de
 *
 */
package org.aksw.simba.quetsal.synopsis;

import java.util.ArrayList;


/**
 * 
 * 
 *
 * @author Sebastian Michel, MPII, smichel@mpi-sb.mpg.de
 *
 */
public class ArrayCollection extends Collection {
    
    private ArrayList<Long> docs;
    
     public ArrayCollection(ArrayList<Long> ret) {
         this.docs = ret;
     }


    public long getDocByRank(int rank) {
        return docs.size();
    }

    public int size() {
        return docs.size();
    }

}
