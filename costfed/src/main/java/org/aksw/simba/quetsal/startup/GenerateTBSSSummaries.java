package org.aksw.simba.quetsal.startup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.aksw.simba.quetsal.util.TBSSSummariesGenerator;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;

/**
 * Generate TBSS summaries for a set of SPARQL endpoints
 * @author Saleem
 */
public class GenerateTBSSSummaries {
	
	public static void main(String[] args) throws IOException, RepositoryException, MalformedQueryException, QueryEvaluationException {
	List<String> endpoints = 	(Arrays.asList(
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ0",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ1",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ2",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ3",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ4",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ5",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ6",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ7",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ8",
//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ9"
			 "http://localhost:8890/sparql",
		  "http://localhost:8890/sparql"
//			 "http://localhost:8892/sparql",
//			 "http://localhost:8893/sparql",
//			 "http://localhost:8894/sparql",
//			 "http://localhost:8895/sparql",
//			 "http://localhost:8896/sparql",
//			 "http://localhost:8897/sparql",
//			 "http://localhost:8898/sparql"
//			// "http://localhost:8899/sparql"
			));

    
	String outputFile = "summaries/quetzal-testb10.n3";
	String namedGraph = "http://aksw.org/fedbench/";  //can be null. in that case all graph will be considered 
	TBSSSummariesGenerator generator = new TBSSSummariesGenerator(outputFile);
	long startTime = System.currentTimeMillis();
	int branchLimit =1;
	generator.generateSummaries(endpoints,namedGraph,branchLimit);
	System.out.println("Data Summaries Generation Time (min): "+ (System.currentTimeMillis()-startTime)/(1000*60));
	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
	
//	outputFile = "summaries\\quetzal-b2.n3";
//	generator = new SummariesGenerator(outputFile);
//	startTime = System.currentTimeMillis();
//	branchLimit =2;
//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
//	
//	outputFile = "summaries\\quetzal-b5.n3";
//	generator = new SummariesGenerator(outputFile);
//	startTime = System.currentTimeMillis();
//	branchLimit =5;
//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
//	
//	outputFile = "summaries\\quetzal-b10.n3";
//	generator = new SummariesGenerator(outputFile);
//	startTime = System.currentTimeMillis();
//	branchLimit =10;
//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
	}

}
 