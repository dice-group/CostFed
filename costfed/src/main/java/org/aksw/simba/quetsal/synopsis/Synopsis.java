package org.aksw.simba.quetsal.synopsis;

import java.io.Serializable;




/*
 * Created on 16.05.2005 by Sebastian Michel
 * Max-Planck Institute for Computer Science
 * 
 * smichel@mpi-sb.mpg.de
 *
 */

/**
 * 
 * 
 *
 * @author Sebastian Michel, MPII, smichel@mpi-sb.mpg.de
 *
 */
public abstract class Synopsis implements Serializable{
	
	
    public abstract double resemblance (Synopsis s);
    public abstract double noveltyWithSynopsis (Synopsis s);
    public abstract long getOriginalSize();

    //public abstract Synopsis FACTORY_METHOD(Collection c);

    public abstract Synopsis union(Synopsis s);
    public abstract Synopsis intersect(Synopsis s);
    
    public abstract long getEstimatedSize();
    
    public abstract int synopsisSizeInBytes();

}
