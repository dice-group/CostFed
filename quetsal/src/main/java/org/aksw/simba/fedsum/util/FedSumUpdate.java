package org.aksw.simba.fedsum.util;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;

public class FedSumUpdate {

	/**
	 * Update Index regularly on a specific date and time
	 * @param lstEndPoints List of SPARQL endpoints
	 * @param  date specific date and time in "dd-MM-yyyy HH:mm:ss"
	 * @param outputFile Output location of index
	 */
	public static void updateIndexAtFixedTime(final List<String> lstEndPoints, Date date, final String outputFile) {
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {public void run() { 	 try {
			FedSumGenerator generator = new FedSumGenerator(outputFile);
			long startTime = System.currentTimeMillis();
			generator.generateSummaries(lstEndPoints);
			System.out.println("Index is secessfully updated to "+ outputFile);
			System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);

		} catch (IOException | RepositoryException | MalformedQueryException | QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 
		}  }, date);
	
		}
	
	/**
	 * Update Index after a fixed interval
	 * @param lstEndPoints List of SPARQL endpoints
	 * @param interval Interval in milliseconds
	 * @param outputFile Output location of index
	 */
	public static void updateIndexAtFixedRate(final List<String> lstEndPoints, long interval, final String outputFile) {
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {public void run() { 	 try {
			FedSumGenerator generator = new FedSumGenerator(outputFile);
			long startTime = System.currentTimeMillis();
			generator.generateSummaries(lstEndPoints);
			System.out.println("Index is secessfully updated to "+ outputFile);
			System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);

		} catch (IOException | RepositoryException | MalformedQueryException | QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		}  }, 0, interval);
	
		}
	

}
