package org.aksw.simba.quetsal.startup;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.aksw.simba.quetsal.util.SummariesUpdate;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;

public class UpdateSummaries {

	 /**
	  * Update Hibiscus summaries
	  * Main startup function  
	  * @param args Input arguments
	  * @throws IOException Io Error
	 * @throws ParseException  Parsing Error
	 * @throws QueryEvaluationException Execution Error
	 * @throws MalformedQueryException Memory Error
	 * @throws RepositoryException Repository Error
	  */
		public static void main(String[] args) throws IOException, ParseException, RepositoryException, MalformedQueryException, QueryEvaluationException 
		{
			List<String> lstEndPoints = 	(Arrays.asList(
					"http://localhost:8890/sparql",
					"http://localhost:8891/sparql",
				     "http://localhost:8892/sparql"
//					"http://localhost:8893/sparql",
//					"http://localhost:8894/sparql",
//					"http://localhost:8895/sparql",
//					"http://localhost:8896/sparql",
//					"http://localhost:8897/sparql",
//					"http://localhost:8898/sparql",
		//		"http://localhost:8899/sparql"
				));
		String outputFile = "D:/workspace/FedSum/summaries/FedSumMotivatingExampleUpdater.n3";
		int branchLimit =5;
			//---------update index on fixed interval of time -------------
	    	  
	    	 //   long interval = 5*1000;
	    	  //  SummariesUpdate.updateIndexAtFixedRate(lstEndPoints,interval,outputFile,branchLimit);
	    	 
	    	   
	    	  //---------update index on a specific Date and time------------------------
	    	    DateFormat dateFormatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	    	    Date date = dateFormatter.parse("22-12-2013 05:04:00");
	    	    SummariesUpdate.updateIndexAtFixedTime(lstEndPoints,date,outputFile,branchLimit);
//	    	    
	    }

}
