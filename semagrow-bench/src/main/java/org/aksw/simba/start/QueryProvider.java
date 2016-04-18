package org.aksw.simba.start;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryProvider {
	Map<String, String> queries_ = new HashMap<String, String>();
	
	public QueryProvider(String path) {
		File f = new File(path);
		if (!f.exists() || !f.isDirectory()) {
			throw new RuntimeException(String.format("%1$s is not a directory", f.getAbsolutePath()));
		}
		List<File> queries = Arrays.asList(f.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isFile();
			}
		}));
		for (File qfl : queries)
		{
			queries_.put(qfl.getName(), Utility.readFile(qfl));
		}
	}
	
	public String getQuery(String queryName) {
		return queries_.get(queryName);
	}
	
	public Map<String, String> getQueries() {
		return queries_;
	}
}
