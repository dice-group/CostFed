/*
 * Copyright (C) 2008-2013, fluid Operations AG
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.fluidops.fedx.util;

import java.net.URI;
import java.net.URL;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Version utility: read the version from the Jar's manifest file.
 * 
 * @author Andreas Schwarte
 *
 */
public class Version {

	protected static Logger log = LoggerFactory.getLogger(Version.class);
	
	/* fields with default values (i.e. if not started from jar) */
	protected static String project = "FedX";
	protected static String date = "88.88.8888";
	protected static String longVersion = "8.8 (build 8888)";
	protected static String build = "8888";
	protected static String version = "FedX 8.8";
	protected static String contact = "info@fluidops.com";
	protected static String companyName = "fluid Operations AG";
	protected static String productName = "fluid FedX";
	
    
	static String getValue(String val, String defaultVal) {
	    return val != null ? val : defaultVal;
	}
	
	static {
		
		try {
			String jarPath = getJarPath();
			
			if (jarPath!=null) {
				
				JarFile jar = new JarFile(jarPath);
				
				Manifest buildManifest = jar.getManifest();
	    	    if(buildManifest!=null) {
	    	    	project = getValue(buildManifest.getMainAttributes().getValue("project"), project);
	    	    	date = getValue(buildManifest.getMainAttributes().getValue("date"), date);
	    	        longVersion = getValue(buildManifest.getMainAttributes().getValue("version"), longVersion);
	    	        build =  getValue(buildManifest.getMainAttributes().getValue("build"), build);		// roughly svn version
	    	        version = getValue(buildManifest.getMainAttributes().getValue("ProductVersion"), version);
	    	        contact =  getValue(buildManifest.getMainAttributes().getValue("ProductContact"), contact);  	       
	    	        companyName = getValue(buildManifest.getMainAttributes().getValue("CompanyName"), companyName);
	    	        productName = getValue(buildManifest.getMainAttributes().getValue("ProductName"), productName);
	    	    }
	    	    
	    	    jar.close();
			}
		} catch (Exception e) {
			log.warn("Error while reading version from jar manifest.", e);
			; 	// ignore
		}
	}
	
	protected static String getJarPath() {

		URL url = Version.class.getResource("/com/fluidops/fedx/util/Version.class");
		String urlPath = url.getPath();
		// url is something like file:/[Pfad_der_JarFile]!/[Pfad_der_Klasse] 
		
		// not a jar, e.g. when started from eclipse
		if (!urlPath.contains("!")) {
			return null;
		}
		
		try {
			URI uri = new URI(url.getPath().split("!")[0]);
			return uri.getPath();
		} catch (Exception e) {
			log.warn("Error while retrieving jar path", e);
			return null;
		}
	}
	
	/**
	 * @return
	 * 		the version string, i.e. 'FedX 1.0 alpha (build 1)'
	 */
	public static String getVersionString() {
		return project + " " + longVersion;
	}
	
	/**
	 * print information to Stdout
	 */
	public static void printVersionInformation() {
		System.out.println("Version Information: " + project + " " + longVersion);
	}


	
	public static String getProject() {
		return project;
	}

	public static String getDate() {
		return date;
	}

	public static String getLongVersion() {
		return longVersion;
	}

	public static String getBuild() {
		return build;
	}

	public static String getVersion() {
		return version;
	}

	public static String getContact() {
		return contact;
	}

	public static String getCompanyName() {
		return companyName;
	}

	public static String getProductName() {
		return productName;
	}
	
	
    /**
     * Prints the version info.
     * @param args
     */
	public static void main(String[] args) {
	    printVersionInformation();
	}
	
}
