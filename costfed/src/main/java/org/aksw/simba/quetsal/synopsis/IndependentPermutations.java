package org.aksw.simba.quetsal.synopsis;
import java.io.Serializable;

import cern.jet.random.engine.MersenneTwister;

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
public class IndependentPermutations implements Serializable {

	private static final long serialVersionUID = 3977577013362047027L;
    
    private  long[] a;  
    private  long[] b;
    private  final long u= 991205981; 
    
    private  int n=-1;
    private long seed;
    
    public IndependentPermutations(int n, long seed) {
        this.n = n;
        this.seed = seed;
        init();
    }
    
    
    
    /**
     * Create n  permutations
     *
     */
    private  void init() {



        if (a==null) {
        //    System.out.println("this.seed = "+this.seed);
            MersenneTwister random = new MersenneTwister((int) this.seed);
		    a = new long[n];
		    b = new long[n];
		    for (int j=0; j<n; j++) {
		        a[j] = random.nextLong();
		        b[j] = random.nextLong();
		   //     System.out.println("a["+j+"]="+a[j]);
		    //    System.out.println("b["+j+"]="+b[j]);
		        while (a[j]<0)  a[j] = random.nextLong();
		        while (b[j]<0)  b[j] = random.nextLong();
		    }  
        }
    }

	/**
	 * Applies the permutation j on the document id (long) 
	 * @param j permutation id
	 * @param id doc id
	 * @return 
	 */
    public  long apply(int j, long id) { 
        
        long result = (long) mod(a[j]*id+b[j], u);
      //  System.out.println(""+j+", "+id +"="+ result );
        return result;
    }

    private static long mod(double x, double y) {
        long i = (int) Math.floor(x / y);
        double x_ = i*y;
        return (long) Math.floor(x-x_);
    }
    
}
