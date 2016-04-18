package com.fluidops.fedx.algebra;

public interface FedXExprVisitor {
	public void meet(StatementTupleExpr ste);
	public void meet(ExclusiveGroup eg);
	public void meet(NJoin nj);
	public void meet(NUnion nu);
	public void meet(SingleSourceQuery ssq);
	public void meet(FedXService fs);
	public void meet(ProjectionWithBindings pwb);
	public void meet(IndependentJoinGroup ijg);
	public void meet(EmptyResult er);
	//public void meet(ExclusiveStatement es);
	//public void meet(CheckStatementPattern csp);
	//public void meet(StatementSourcePattern ssp);
}
