package org.aksw.costfed.stats;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;

/**
 * @author Tommaso Soru <tsoru@informatik.uni-leipzig.de>
 *
 */
public class Stats {

	public static void main(String[] args) throws FileNotFoundException {

		String endpoint = args[0];
		String name = args[1];
		String appendto = args[2];
		
		double netMeanS = 0.0, netStdevS = 0.0, netMeanO = 0.0, netStdevO = 0.0; 
		int nProp = 0;
		
		for (int j = 0; true; j++) {

			ResultSet rs = query(endpoint, Queries.GET_ALL_PROPERTIES,
					j * 10000);

			int i;
			for (i = 0; rs.hasNext(); i++) {
				String p = rs.next().get("x").toString();
				System.out.println("Processing: " + p);
				
				Info infoS = process(endpoint, Queries.SUBJECT_FREQUENCIES, p);				
				netMeanS += infoS.mean;
				netStdevS += infoS.stdev;

				Info infoO = process(endpoint, Queries.OBJECT_FREQUENCIES, p);				
				netMeanO += infoO.mean;
				netStdevO += infoO.stdev;


			}
			System.out.println("Gathered " + i + " properties");
			
			nProp += i;
			if (i == 0)
				break;
		}
		
		netMeanS /= nProp;
		netStdevS /= nProp;
		netMeanO /= nProp;
		netStdevO /= nProp;
		
		PrintWriter pw = new PrintWriter(new FileOutputStream(new File(appendto), true));
		String line = String.format("%s\t%f\t%f\t%f\t%f", name, netMeanS, netStdevS, netMeanO, netStdevO);
		System.out.println(line);
		pw.println(line);
		pw.close();
		

	}
	
	
	private static Info process(String endpoint, String query, String p) {
		
		long sum = 0, n = 0, sumsq = 0;
		for (int j2 = 0; true; j2++) {
			ResultSet rsPs = query(endpoint,
					query, p, j2 * 10000);
			int i2;
			for (i2 = 0; rsPs.hasNext(); i2++) {
				int f = rsPs.next().getLiteral("x").getInt();
				sum += f;
				sumsq += f * f;
//				System.out.println(sum + "," + sumsq);
			}
			n += i2;
			if (i2 == 0)
				break;
		}
		double mean = ((double) sum / n);
		System.out.println("Mean is " + mean);
//		System.out.println(sumsq + "," + sum + "," + n);
		double stdev = n==1 ? 0 : Math.sqrt(Math.abs(sumsq - (sum * sum) / (double) n)
				* (1.0d / (n - 1)));
		System.out.println("StDev is " + stdev);
		
		return new Info(mean, stdev);

	}

	public static ResultSet query(String endpoint, String query,
			Object... params) {

		query = String.format(query, params);
		System.out.println(query);

		Query sparqlQuery = QueryFactory.create(query, Syntax.syntaxARQ);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint,
				sparqlQuery, "http://aksw.org/fedbench/");
		try {

			ResultSet rs = qexec.execSelect();
			return rs;

		} catch (Exception e1) {
			e1.printStackTrace();
			return null;
		}

	}

}

class Info {
	double mean;
	double stdev;
	Info(double mean, double stdev) {
		this.mean = mean;
		this.stdev = stdev;
	}
}
