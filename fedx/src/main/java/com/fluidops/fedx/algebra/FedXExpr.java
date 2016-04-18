package com.fluidops.fedx.algebra;

public interface FedXExpr {
	public void visit(FedXExprVisitor v);
}
