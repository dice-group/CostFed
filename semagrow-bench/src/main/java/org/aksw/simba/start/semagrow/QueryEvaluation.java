package org.aksw.simba.start.semagrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.aksw.simba.start.QueryProvider;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfig;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.config.RepositoryRegistry;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.config.SailConfigException;

import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.repository.SemagrowRepository;


public class QueryEvaluation {
	protected static final Logger log = Logger.getLogger(QueryEvaluation.class);
	
	QueryProvider qp;
	private RepositoryFactory repoFactory;
	private SemagrowRepository repository;
	
	public QueryEvaluation() throws Exception {
		qp = new QueryProvider("../queries/");
		SemagrowRepositoryConfig repoConfig = getConfig();
		
		repoFactory = RepositoryRegistry.getInstance().get(repoConfig.getType());
        repository = (SemagrowRepository) repoFactory.getRepository(repoConfig);
        repository.initialize();
        
//        // remove CSV and TSV format due to bug: literals are recognized as URIs if they contain a substring parsable as URI.
//        TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
//        registry.remove(registry.get(TupleQueryResultFormat.CSV));
//        registry.remove(registry.get(TupleQueryResultFormat.TSV));
//
//        BooleanQueryResultParserRegistry booleanRegistry = BooleanQueryResultParserRegistry.getInstance();
//        booleanRegistry.remove(booleanRegistry.get(BooleanQueryResultFormat.JSON));
	}
	
	class SyncTupleQueryResultHandler implements TupleQueryResultHandler {
		long resultCount = 0;
		
		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
			
		}

		@Override
		public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
			
		}

		@Override
		public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
			
		}

		@Override
		public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
			resultCount++;
		}

		@Override
		public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {
			
		}
		
	}
	
	public List<List<Object>> evaluate(String queries) throws Exception {
		List<List<Object>> report = new ArrayList<List<Object>>();
				
		List<String> qnames = Arrays.asList(queries.split(" "));
		for (String curQueryName : qnames)
		{
			List<Object> reportRow = new ArrayList<Object>();
			report.add(reportRow);
			String curQuery = qp.getQuery(curQueryName);
			reportRow.add(curQueryName);
			
			long startTime = System.currentTimeMillis();
			//ParsedOperation pO = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, curQuery, null);
			RepositoryConnection repCon = this.repository.getConnection();
			try {
				Query tempq = repCon.prepareQuery(QueryLanguage.SPARQL, curQuery);
				TupleQuery q = (TupleQuery)tempq;
				
				SyncTupleQueryResultHandler rhandler = new SyncTupleQueryResultHandler();
	            q.evaluate(rhandler);
	            		
			    long runTime = System.currentTimeMillis() - startTime;
			    reportRow.add((Long)rhandler.resultCount); reportRow.add((Long)runTime);

			    log.info(curQueryName + ": Query exection time (msec): "+ runTime + ", Total Number of Records: " + rhandler.resultCount);
			} catch (Exception e) {
				reportRow.add(null); reportRow.add(null);
			} finally {
				repCon.close();
	        }
		}
		return report;
	}
	
    public void shutDown(){
        try {
            this.repository.shutDown();
            RepositoryRegistry.getInstance().remove(repoFactory);
        } catch (RepositoryException ex) {}
    }
    
    private SemagrowRepositoryConfig getConfig() {

        try {
            File file = FileUtils.getFile("resources/repository.ttl");
            Graph configGraph = parseConfig(file);
            RepositoryConfig repConf = RepositoryConfig.create(configGraph, null);
            repConf.validate();
            RepositoryImplConfig implConf = repConf.getRepositoryImplConfig();
            return (SemagrowRepositoryConfig)implConf;
        } catch (RepositoryConfigException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        } catch (SailConfigException | IOException | NullPointerException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        }
    }
    
    protected Graph parseConfig(File file) throws SailConfigException, IOException {

        RDFFormat format = Rio.getParserFormatForFileName(file.getAbsolutePath());
        if (format==null)
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        RDFParser parser = Rio.createParser(format);
        Graph model = new GraphImpl();
        parser.setRDFHandler(new StatementCollector(model));
        InputStream stream = new FileInputStream(file);

        try {
            parser.parse(stream, file.getAbsolutePath());
        } catch (Exception e) {
            throw new SailConfigException("Error parsing file!");
        }

        stream.close();
        return model;
    }
    
	public static void main(String[] args) throws Exception {
		log.info("Start");
		File repfile = args.length > 0 ? new File(args[0]) : null;

		//List<List<Object>> report = multyEvaluate("S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 C2 C3 C4 C7 C8 C10", 1);
		List<List<Object>> report = multyEvaluate("S1 S2", 1);
		String r = printReport(report);
		log.info(r);
		if (null != repfile) {
			FileUtils.write(repfile, r);
		}

		System.exit(0);
	}

	static List<List<Object>> multyEvaluate(String queries, int num) throws Exception {
		QueryEvaluation qeval = new QueryEvaluation();
		try {
			List<List<Object>> report = null;
			for (int i = 0; i < num; ++i) {
				List<List<Object>> subReport = qeval.evaluate(queries);
				if (i == 0) {
					report = subReport;
				} else {
					assert(report.size() == subReport.size());
					for (int j = 0; j < subReport.size(); ++j) {
						List<Object> subRow = subReport.get(j);
						List<Object> row = report.get(j);
						row.add(subRow.get(2));
					}
				}
			}
			
			return report;
		} finally {
			qeval.shutDown();
		}
	}
	
	static String printReport(List<List<Object>> report) {
		if (report.isEmpty()) return "";
		
		StringBuilder sb = new StringBuilder();
		sb.append("Query,#Results");
		
		List<Object> firstRow = report.get(0);
		for (int i = 2; i < firstRow.size(); ++i) {
			sb.append(",Sample #").append(i - 2);
		}
		sb.append("\n");
		for (List<Object> row : report) {
			for (Object cell : row) {
				sb.append(cell);
				if (cell != row.get(row.size() - 1)) {
					sb.append(",");
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}
