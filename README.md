# CostFed
 Cost-Based Query Optimization for SPARQL Endpoint Federation
.

# Quick fedbench Start #
* modify costfed/src/main/java/org/aksw/simba/start/QueryEvaluation.java :
	correct host, endpoints and queries variables
* update summary file costfed/summaries/sum-localhost.n3 if needed or generate new one
* update the path to the corresponded summary file in the property file (e.g. costfed.props)
* start fedbench  org.aksw.simba.start.QueryEvaluation with argument - name of property file
  e.g.
	org.aksw.simba.start.QueryEvaluation fedx.props runs original fedx engine
	org.aksw.simba.start.QueryEvaluation costfed.props runs costfed engine

# Summary generation #
* modify costfed/src/main/java/org/aksw/simba/quetsal/util/TBSSSummariesGenerator.java
	correct host, endpoints and outputFile variables
* run org.aksw.simba.quetsal.util.TBSSSummariesGenerator