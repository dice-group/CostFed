## System Requirements

The following external dependencies must be satisfied:

1. git >= 1.8.1.4
2. java >= 1.8
3. maven >= 3.0.3

Before testing you need to generate a repository description.
Use org.aksw.simba.quetsal.util.SemagrowSummariesGenerator class after fixing host, endpoints and outputFile variables direct in the source file.
you can use already generated repository descripton https://github.com/apotocki/Semagrow-LargeRDFBench/blob/master/resources/semagrow.ttl after fixing endpint uris.

Test perfomes by class org.aksw.simba.start.semagrow.QueryEvaluation. It takes an output file name as the first agrument.

usage example:
mvn exec:java -Dexec.mainClass="org.aksw.simba.start.semagrow.QueryEvaluation" -Dexec.args=results/test.csv

To generate report use report.py script

Report file: 
https://github.com/apotocki/Semagrow-LargeRDFBench/blob/master/results/complete-report.csv

