package org.aksw.simba.quetsal.core.algebra;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.structures.QueryInfo;

public class HashJoin extends NJoin {
	private static final long serialVersionUID = -677747490612345017L;

	private static List<TupleExpr> createCollection(TupleExpr leftArg, TupleExpr rightArg) {
		List<TupleExpr> result = new ArrayList<TupleExpr>();
		result.add(leftArg);
		result.add(rightArg);
		return result;
	}
	
	public HashJoin(TupleExpr leftArg, TupleExpr rightArg, QueryInfo qi) {
		super(createCollection(leftArg, rightArg), qi);
	}
	
	public TupleExpr getLeftArg() {
		return this.getArg(0);
	}
	
	public TupleExpr getRightArg() {
		return this.getArg(1);
	}
}
