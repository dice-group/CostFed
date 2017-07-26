package org.aksw.simba.quetsal.startup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.aksw.simba.quetsal.util.HibiscusSummariesGenerator;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Test FedSummaries Generator for a set of SPARQL endpoints
 * @author Saleem
 */
public class GenerateHibiscusSummaries {
	
	public static void main(String[] args) throws IOException, RepositoryException, MalformedQueryException, QueryEvaluationException {
	List<String> endpoints = 	(Arrays.asList(

			 "http://localhost:8890/sparql",
			  "http://localhost:8891/sparql",
			 "http://localhost:8892/sparql"
//			 "http://localhost:8893/sparql",
//			 "http://localhost:8894/sparql",
//			 "http://localhost:8895/sparql",
//			 "http://localhost:8896/sparql",
//			 "http://localhost:8897/sparql",
//			 "http://localhost:8898/sparql",
//			 "http://localhost:8899/sparql"
			));

	String outputFile = "D:/workspace/HiBISCus/summaries/OntoSum-UOBM-10-owlim.n3";
	String namedGraph = "http://aksw.org/ontosum";  //can be null. in that case all graph will be considered 
	HibiscusSummariesGenerator generator = new HibiscusSummariesGenerator(outputFile);
	long startTime = System.currentTimeMillis();
	generator.generateSummaries(endpoints,namedGraph);
	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
	}

}
 