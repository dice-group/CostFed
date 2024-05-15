package org.aksw.simba.quetsal.util;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple command-line runner able to generate summaries from a list of SPARQL endpoint URLs.
 *   - args[0] must be the output file, e.g. "output.ttl"
 *   - the following arguments must be the list of all endpoints for which to generate the statistics
 *   
 * @author Thomas Francart thomas.francart@sparna.fr
 *
 */
public class TBSSSummariesGeneratorCommandLineRunner {
	static Logger log = LoggerFactory.getLogger(TBSSSummariesGenerator.class);
	
	public static void main(String[] args) throws IOException, URISyntaxException {		
		
		String outputFile = args[0];
		log.info("Will generate data summaries in output file : " + outputFile);
		
		String namedGraph = null;
		TBSSSummariesGenerator generator = new TBSSSummariesGenerator(outputFile);
		List<String> endpoints = new ArrayList<String>(Arrays.asList(args));
		// remove first parameter corresponding to output file
		endpoints.remove(0);
		
		long startTime = System.currentTimeMillis();
		int branchLimit = 4;
		generator.generateSummaries(endpoints, namedGraph, branchLimit);
		log.info("Data Summaries Generation Time (min): " + (double)(System.currentTimeMillis() - startTime) / (1000 * 60));
		log.info("Data Summaries are sucessfully stored at " + outputFile);
	}

}