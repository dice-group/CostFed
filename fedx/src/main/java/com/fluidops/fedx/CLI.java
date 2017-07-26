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

package com.fluidops.fedx;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.sail.FedXSailRepository;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.EndpointFactory;
import com.fluidops.fedx.util.QueryStringUtil;
import com.fluidops.fedx.util.Version;


/**
 * The command line interface for federated query processing with FedX.
 * 
 * <code>
 * Usage:
 * > FedX [Configuration] [Federation Setup] [Output] [Queries]
 * > FedX -{help|version}
 * 
 * WHERE
 * [Configuration] (optional)
 * Optionally specify the configuration to be used
 *      -c path/to/fedxConfig
 *      -verbose {0|1|2|3}
 *      -logtofile
 *      -p path/to/prefixConfig
 *      -planOnly
 *      
 * [Federation Setup] (optional)
 * Specify one or more federation members
 *      -s urlToSparqlEndpoint
 *      -l path/to/NativeStore
 *      -d path/to/dataconfig.ttl
 *      
 * [Output] (optional)
 * Specify the output options, default stdout. Files are created per query to results/%outputFolder%/q_%id%.{json|xml},
 * where the outputFolder is the current timestamp, if not specified otherwise.
 *      -f {STDOUT,JSON,XML}
 *      -folder outputFolder
 *      
 * [Queries]
 * Specify one or more queries, in file: separation of queries by empty line
 *      -q sparqlquery
 *      @q path/to/queryfile
 *      
 * Notes:
 * The federation members can be specified explicitly (-s,-l,-d) or implicitly as 'dataConfig' 
 * via the fedx configuration  (-f)
 * 
 * If no PREFIX declarations are specified in the configurations, the CLI provides
 * some common PREFIXES, currently rdf, rdfs and foaf. 
 * </code>
 * 
 * 
 * @author Andreas Schwarte
 *
 */
public class CLI {

	protected enum OutputFormat { STDOUT, JSON, XML; }
	
	Config config = null;
	
	protected String fedxConfig=null;
	protected int verboseLevel=0;
	protected boolean logtofile = false;
	protected boolean planOnly = false;
	protected String prefixDeclarations = null;
	
	protected OutputFormat outputFormat = OutputFormat.STDOUT;
	protected List<String> queries = new ArrayList<String>();
	protected FedXSailRepository repo = null;
	protected String outFolder = null;
	
	final List<String> args;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new CLI(args).run();
		} catch (Exception e) {
			System.out.println("Error while using the FedX CLI. System will exit. \nDetails: " + e.getMessage());
			System.exit(1);
		}
	}

	CLI(String[] args) {
	    this.args = new LinkedList<String>(Arrays.asList(args));;
	}
	
	class CLIEndpointListProvider implements EndpointListProvider
	{
	    protected List<Endpoint> endpoints = new ArrayList<Endpoint>();
	    
        @Override
        public List<Endpoint> getEndpoints(FedX federation) {
            String arg = args.get(0);
            
            if (arg.equals("-s")) {
                readArg(args);      // remove -s
                String url = readArg(args,"urlToSparqlEndpoint");
                try {
                    Endpoint endpoint = EndpointFactory.loadSPARQLEndpoint(config, federation.getHttpClient(), url);
                    endpoints.add(endpoint);
                } catch (FedXException e) {
                    error("SPARQL endpoint " + url + " could not be loaded: " + e.getMessage(), false);
                }           
            }
            
            else if (arg.equals("-l")) {
                readArg(args);      // remove -l
                String path = readArg(args,"path/to/NativeStore");
                try {
                    Endpoint endpoint = EndpointFactory.loadNativeEndpoint(config, path);
                    endpoints.add(endpoint);
                } catch (FedXException e) {
                    error("NativeStore " + path + " could not be loaded: " + e.getMessage(), false);
                }
            }
            
            else if (arg.equals("-d")) {
                readArg(args);      // remove -d
                String dataConfig = readArg(args,"path/to/dataconfig.ttl");
                try {
                    List<Endpoint> ep = EndpointFactory.loadFederationMembers(config, federation.getHttpClient(), new File(dataConfig));
                    endpoints.addAll(ep);
                } catch (FedXException e) {
                    error("Data config '" + dataConfig + "' could not be loaded: " + e.getMessage(), false);
                }
            }
            
            else {          
                error("Expected at least one federation member (-s, -l, -d), was: " + arg, false);
            }
            
            // generic checks
            if (endpoints.size() == 0) {
                error("No federation members specified. At least one data source is required.", true);
            }
            
            if (config.getDataConfig() != null) {
                // currently there is no duplicate detection, so the following is a hint for the user
                // can cause problems if members are explicitly specified (-s,-l,-d) and via the fedx configuration
                if (endpoints.size() > 0) {
                    System.out.println("WARN: Mixture of implicitely and explicitely specified federation members, dataConfig used: " + config.getDataConfig());
                }
                try {
                    List<Endpoint> additionalEndpoints = EndpointFactory.loadFederationMembers(config, federation.getHttpClient(), new File(config.getDataConfig()));
                    endpoints.addAll(additionalEndpoints);
                } catch (FedXException e) {
                    error("Failed to load implicitly specified data sources from fedx configuration. Data config is: " + config.getDataConfig() + ". Details: " + e.getMessage(), false);
                }
            }
            
            
            return endpoints;
        }

        @Override
        public void close() {
            
        }
	}
	
	public void run() {
		configureRootLogger();
		
		System.out.println("FedX Cli " + Version.getLongVersion());
		
		// parse the arguments and construct config
		parse();
		
		// activate logging to stdout if verbose is set
		configureLogging();
						
        if (queries.size() == 0) {
            error("No queries specified", true);
        }
        
		// setup the federation
		try {
			repo = FedXFactory.initializeFederation(config, new CLIEndpointListProvider());
		} catch (FedXException e) {
			error("Problem occured while setting up the federation: " + e.getMessage(), false);
		}
		
		SailRepositoryConnection conn = repo.getConnection();
		FedXConnection fconn = (FedXConnection)conn.getSailConnection();
		QueryManager qm = fconn.getQueryManager();

	      // initialize default prefix declarations (if the user did not specify anything)
        if (config.getPrefixDeclarations() == null) {
            initDefaultPrefixDeclarations(qm);
        }
        
		int count=1;
		for (String queryString : queries) {
			
			try {
				if (planOnly) {
					System.out.println(qm.getQueryPlan(queryString, fconn.getSummary()));
				} else {
					System.out.println("Running Query " + count);
					runQuery(conn, queryString, count);
				}
			} catch (Exception e) {
				error("Query " + count + " could not be evaluated: \n" + e.getMessage(), false);
			}
			count++;
		}
		
		System.out.println("Done.");
		System.exit(0);
 	}
	
	protected void parse() {
		if (args.size() == 0) {
			printUsage(true);
		}
		
		if (args.size() == 1 && args.get(0).equals("-help"))  {
			printUsage(true);		
		}
				
	
		parseConfiguaration(args, false);
		
		// initialize config
		try {
			config = new Config(fedxConfig); // fedxConfig may be null (default values)
			if (prefixDeclarations != null) {
			    config.set("prefixDeclarations", prefixDeclarations);	// override in config
			}
		} catch (FedXException e) {
			error("Problem occured while setting up the federation: " + e.getMessage(), false);
		}		
		
		parseOutput(args);
		parseQueries(args);
		
		if (outFolder==null) {
			outFolder = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Date());
		}

	}
	
	/**
	 * Parse the FedX Configuration,
	 *  1) -c path/to/fedxconfig.prop
	 *  2) -verbose [%lvl$]
	 *  3) -logtofile
	 *  4) -p path/to/prefixDeclaration.prop
	 *  5) -planOnly	 
	 *  
	 *  WHERE lvl is 0=off (default), 1=INFO, 2=DEBUG, 3=TRACE
	 *  
	 * @param args
	 */
	protected void parseConfiguaration(List<String> args, boolean printError) {
		String arg = args.get(0);
		
		// fedx config
		if (arg.equals("-c")) {
			readArg(args);											// remove -c
			fedxConfig = readArg(args, "path/to/fedxConfig.ttl");	// remove path
		}
		
		// verbose
		else if (arg.equals("-verbose")) {
			verboseLevel = 1;
			readArg(args);
			try {
				verboseLevel = Integer.parseInt(args.get(0));
				readArg(args);
			} catch (Exception numberFormat) {
				; // ignore
			}
		}
		
		// logtofile
		else if (arg.equals("-logtofile")) {
			readArg(args);
			logtofile = true;
		}
		
		// prefixConfiguration
		else if (arg.equals("-p")) {
			readArg(args);
			prefixDeclarations = readArg(args, "path/to/prefixDeclarations.prop");
		}
		
		else if (arg.equals("-planOnly")) {
			readArg(args);
			planOnly = true;
		}
		
		else {
			if (printError)
				error("Unxpected Configuration Option: " + arg, false);
			else
				return;
		}
		
		parseConfiguaration(args, false);
	}
	
	/**
	 * Parse output options
	 *  1) Format: -f {STDOUT,XML,JSON}
	 *  2) OutputFolder: -folder outputFolder 
	 * 
	 * @param args
	 */
	protected void parseOutput(List<String> args) {
		
		String arg = args.get(0);
		
		if (arg.equals("-f")) {
			readArg(args);		// remove -s
			
			String format = readArg(args, "output format {STDOUT, XML, JSON}");
			
			if (format.equals("STDOUT"))
				outputFormat = OutputFormat.STDOUT;
			else if (format.equals("JSON"))
				outputFormat = OutputFormat.JSON;
			else if (format.equals("XML")) {
				outputFormat = OutputFormat.XML;
			}
			else {
				error("Unexpected output format: " + format + ". Available options: STDOUT,XML,JSON", false);
			}
		}
		
		else if (arg.equals("-folder")) {
			readArg(args);		// remove -folder
			
			outFolder = readArg(args, "outputFolder");
		}
		
		else {
			return;
		}
		
		parseOutput(args);
	}
	
	
	/**
	 * Parse query input
	 *  1) Querystring: -q SparqlQuery
	 *  2) File: @q path/to/QueryFile
	 *  
	 * @param args
	 */
	protected void parseQueries(List<String> args) {
		String arg = args.get(0);
		
		if (arg.equals("-q")) {
			readArg(args);	// remove -q
			String query = readArg(args, "SparqlQuery");
			queries.add(query);
		}
		
		else if (arg.equals("@q")) {
			readArg(args);	// remove @q
			String queryFile = readArg(args, "path/to/queryFile");
			try {	
				List<String> q = QueryStringUtil.loadQueries(queryFile);
				queries.addAll(q);
			} catch (IOException e) {
				error("Error loading query file '" + queryFile + "': " + e.getMessage(), false);
			}
		}
		
		else {
			error("Unexpected query argument: " + arg, false);
		}
		
		if (args.size()>0)
			parseQueries(args);
	}
	
	protected String readArg(List<String> args, String... expected) {
		if (args.size()==0)
			error("Unexpected end of args, expected: " + expected, false);
		return args.remove(0);
	}
	
	/**
	 * initializes default prefix declarations from com.fluidops.fedx.commonPrefixesCli.prop
	 */
	protected void initDefaultPrefixDeclarations(QueryManager qm) {
		Properties props = new Properties();
		try	{
			props.load(CLI.class.getResourceAsStream("/com/fluidops/fedx/commonPrefixesCli.prop"));
		} catch (IOException e)	{
			throw new FedXRuntimeException("Error loading prefix properties: " + e.getMessage());
		}
		
		for (String ns : props.stringPropertyNames()) {
			qm.addPrefixDeclaration(ns, props.getProperty(ns));  	// register namespace/prefix pair
		}
	}
	
	protected void runQuery(SailRepositoryConnection conn, String queryString, int queryId) throws QueryEvaluationException {
			
		TupleQuery query;
		try {
			query = conn.prepareTupleQuery(queryString);
		} catch (MalformedQueryException e) {
			throw new QueryEvaluationException("Query is malformed: " + e.getMessage());
		} 
		int count = 0;
		long start = System.currentTimeMillis();
		
		TupleQueryResult res = query.evaluate();
				
		if (outputFormat == OutputFormat.STDOUT) {
			while (res.hasNext()) {
				System.out.println(res.next());
				count++;
			}
		}
		
		else if (outputFormat == OutputFormat.JSON) {
			
			File out = new File("results/"+ outFolder + "/q_" + queryId + ".json");
			out.getParentFile().mkdirs();
			
			System.out.println("Results are being written to " + out.getPath());
			
			try {
				SPARQLResultsJSONWriter w = new SPARQLResultsJSONWriter(new FileOutputStream(out));
				w.startQueryResult(res.getBindingNames());
				
				while (res.hasNext()) {
					w.handleSolution(res.next());
					count++;
				}
				
				w.endQueryResult();
			} catch (IOException e) {
				error("IO Error while writing results of query " + queryId + " to JSON file: " + e.getMessage(), false);
			} catch (TupleQueryResultHandlerException e) {
				error("Tuple result error while writing results of query " + queryId + " to JSON file: " + e.getMessage(), false);
			}
		}
		
		else if (outputFormat == OutputFormat.XML) {
			
			File out = new File("results/" + outFolder + "/q_" + queryId + ".xml");
			out.getParentFile().mkdirs();
			
			System.out.println("Results are being written to " + out.getPath());
			
			try {
				SPARQLResultsXMLWriter w = new SPARQLResultsXMLWriter(new FileOutputStream(out));
				w.startQueryResult(res.getBindingNames());
				
				while (res.hasNext()) {
					w.handleSolution(res.next());
					count++;
				}
				
				w.endQueryResult();
				
			} catch (IOException e) {
				error("IO Error while writing results of query " + queryId + " to XML file: " + e.getMessage(), false);
			} catch (TupleQueryResultHandlerException e) {
				error("Tuple result error while writing results of query " + queryId + " to JSON file: " + e.getMessage(), false);
			}
		}
		
		long duration = System.currentTimeMillis() - start;		// the duration in ms
		
		System.out.println("Done query " + queryId + ": duration=" + duration + "ms, results=" + count);
	}
	
	
	
	/**
	 * Print an error and exit
	 * 
	 * @param errorMsg
	 */
	protected void error(String errorMsg, boolean printHelp) {
		System.out.println("ERROR: " + errorMsg);
		if (printHelp) {
			System.out.println("");
			printUsage();
		}
		System.exit(1);
	}
	
	
	/**
	 * Print the documentation
	 */
	protected void printUsage(boolean... exit) {
		
		System.out.println("Usage:");
		System.out.println("> FedX [Configuration] [Federation Setup] [Output] [Queries]");
		System.out.println("> FedX -{help|version}");
		System.out.println("");
		System.out.println("WHERE");
		System.out.println("[Configuration] (optional)");
		System.out.println("Optionally specify the configuration to be used");
		System.out.println("\t-c path/to/fedxConfig");
		System.out.println("\t-verbose {0|1|2|3}");
		System.out.println("\t-logtofile");
		System.out.println("\t-p path/to/prefixDeclarations");
		System.out.println("\t-planOnly");
		System.out.println("");
		System.out.println("[Federation Setup] (optional)");
		System.out.println("Specify one or more federation members");
		System.out.println("\t-s urlToSparqlEndpoint");
		System.out.println("\t-l path/to/NativeStore");
		System.out.println("\t-d path/to/dataconfig.ttl");
		System.out.println("");
		System.out.println("[Output] (optional)");
		System.out.println("Specify the output options, default stdout. Files are created per query to results/%outputFolder%/q_%id%.{json|xml}, where the outputFolder is the current timestamp, if not specified otherwise.");
		System.out.println("\t-f {STDOUT,JSON,XML}");
		System.out.println("\t-folder outputFolder");
		System.out.println("");
		System.out.println("[Queries]");
		System.out.println("Specify one or more queries, in file: separation of queries by empty line");
		System.out.println("\t-q sparqlquery");
		System.out.println("\t@q path/to/queryfile");
		System.out.println("");
		System.out.println("Examples:");
		System.out.println("Please have a look at the examples attached to this package.");
		System.out.println("");
		System.out.println("Notes:");
		System.out.println("The federation members can be specified explicitely (-s,-l,-d) or implicitely as 'dataConfig' via the fedx configuration (-f)");
		System.out.println("If no PREFIX declarations are specified in the configurations, the CLI provides some common PREFIXES, currently rdf, rdfs and foaf. ");
		
		if (exit.length!=0 && exit[0])
			System.exit(0);
	}
	
	
	/**
	 * Activate logging if -verbose is enabled.
	 * 
	 * Verbose level: 0=off (default), 1=INFO, 2=DEBUG, 3=ALL
	 */
	protected void configureLogging() {
		/*
		//Logger rootLogger = Logger.getRootLogger();
		Logger l = LoggerFactory.getLogger("com.fluidops.fedx");
		Logger rootLogger = l.getRootLogger();
		if (verboseLevel>0) {
			//Logger pkgLogger = rootLoggerFactory.getLoggerRepository().getLogger("com.fluidops.fedx"); 
			if (verboseLevel==1) {
				rootLogger.setLevel(Level.INFO);
				l.setLevel(Level.INFO);
			} else if (verboseLevel==1) {
				rootLogger.setLevel(Level.DEBUG);
				l.setLevel(Level.DEBUG);
			}	else if (verboseLevel>2) {
				rootLogger.setLevel(Level.ALL);
				l.setLevel(Level.ALL);
			}
				
			if (logtofile) {
				try {
					l.addAppender( new FileAppender(new PatternLayout("%5p %d{yyyy-MM-dd hh:mm:ss} [%t] (%F:%L) - %m%n"), "logs/fedx_cli.log"));
				} catch (IOException e) {
					System.out.println("WARN: File Logging could not be initialized: " + e.getMessage());
				}				
			} else {
				l.addAppender(new ConsoleAppender(new PatternLayout("%5p [%t] (%F:%L) - %m%n")));
			}
		}		
		*/
	}
	
	protected void configureRootLogger() {
		/*
		Logger rootLogger = Logger.getRootLogger();
		if (!rootLogger.getAllAppenders().hasMoreElements()) {
			rootLogger.setLevel(Level.ALL); 
			rootLogger.addAppender(new NullAppender() );      
		}
		*/
	}
}
