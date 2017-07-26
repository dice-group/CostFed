package org.aksw.simba.bigrdfbench.util;

import java.io.File;
import java.io.IOException;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

public class ResultsLoader {
	public static  RepositoryConnection con = null;
	public static void loadResults(String RDFresultsFile) {
		File curfile = new File ("memorystore.data");
		curfile.delete();
		File fileDir = new File("results");
		Repository myRepository = new SailRepository( new MemoryStore(fileDir) );
		try {
			myRepository.initialize();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		    File file = new File(RDFresultsFile);
			
			try {
				con = myRepository.getConnection();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}
			   try {
				con.add(file, "aksw.org.simba", RDFFormat.N3);
			} catch (RDFParseException e) {
				e.printStackTrace();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			  
			
		}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
