package org.aksw.simba.start;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Utility {
	
	public static String readFile(File path) {
		try {
		    String         line = null;
		    StringBuilder  stringBuilder = new StringBuilder();
		    String         ls = System.getProperty("line.separator");
		    BufferedReader reader = new BufferedReader(new FileReader(path));
		    try {
			    while( ( line = reader.readLine() ) != null ) {
			        stringBuilder.append( line );
			        stringBuilder.append( ls );
			    }
		
			    return stringBuilder.toString();
		    } finally {
		    	try { reader.close(); } catch (IOException ignore) {}
		    }
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
}
