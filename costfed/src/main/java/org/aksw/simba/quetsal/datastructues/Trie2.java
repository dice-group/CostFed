package org.aksw.simba.quetsal.datastructues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Trie2 {
	public static class Node {
		long hits = 0;
		long unique = 0;
		Map<String, Node> children = new HashMap<String, Node>();
		
		private Node() {
			
		}
		
		private Node(String tname) {
			unique = 1;
			hits = 1;
			children.put(tname, null);
		}
		
		private void insertPair(String k, Node v) {
			children.put(k, v);
		}
		
		public boolean insert(String word) {
			boolean result = false;
			++hits;
			boolean childFound = false;
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				int i = 0;
				String k = e.getKey();
				if (k.isEmpty()) continue;
				// find common part
				for (; i < k.length() && i < word.length(); ++i) {
					if (k.charAt(i) != word.charAt(i)) break;
				}
				if (i != 0) {
					childFound = true;
					String tail1 = word.substring(i);
					Node curChild = e.getValue();
					if (i == k.length()) {
						if (!tail1.isEmpty()) {
							result = curChild.insert(tail1);
						} else {
							++curChild.hits;
						}
					} else {
						String commonPrefix = k.substring(0, i);
						String tail0 = k.substring(i);
						
						children.remove(k);
						Node newChild = new Node();
						newChild.hits = curChild.hits;
						newChild.unique = curChild.unique;
						children.put(commonPrefix, newChild);
						newChild.insertPair(tail0, curChild);
						newChild.insertPair(tail1, new Node(""));
						++newChild.hits;
						++newChild.unique;
						result = true;
					}
					break;
				}
			}
			if (!childFound) { // no common prefix => new root
				insertPair(word, new Node(""));// terminator mark
				result = true;
			}
			
			if (result) {
				++unique;
			}
			return result;
		}
		
		void updateList(String nm, long hits, int maxsize, List<Pair<String, Long>> result) {
			Pair<String, Long> v = new Pair<String, Long>(nm, hits);
			int pos = Collections.binarySearch(result, v, (cpl, cpr) -> -Long.compare(cpl.getSecond(), cpr.getSecond()));
			
			if (pos < 0) pos = -(pos + 1);
			if (pos < maxsize) {
				result.add(pos, v);
				if (result.size() > maxsize) {
					result.remove(result.size() - 1);
				}
			}
		}
		
		public void findMostHittable(String path, int maxsize, List<Pair<String, Long>> result)
		{
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				Node nd = e.getValue();
				if (nd == null) { // terminator
					updateList(path, hits, maxsize, result);
				} else {
					if (result.size() < maxsize || result.get(result.size() - 1).getSecond() < nd.hits) {
						nd.findMostHittable(path + e.getKey(), maxsize, result);
					}
				}
			}
		}
		
		public void gatherPrefixes(int max, String curPrefix, List<Tuple3<String, Long, Long>> result)
		{
			List<Tuple3<String, Long, Long>> tempResults = new ArrayList<Tuple3<String, Long, Long>>();
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				Node nd = e.getValue();
				if (nd == null) { // terminator
					tempResults.add(new Tuple3<String, Long, Long>(curPrefix, (long)1, hits));
				} else {
					tempResults.add(new Tuple3<String, Long, Long>(curPrefix + e.getKey(), nd.unique, nd.hits));
				}
			}
		}
		
		/*
		public void findMostHittable(List<Map.Entry<String, Node>> path) {
			Map.Entry<String, Node> maxentry = null;
			int maxval = 0;
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				Node nd = e.getValue();
				int val = (null != nd) ? e.getValue().hits : hits;
				if (val > maxval) {
					maxval = val;
					maxentry = e;
				}
			}
			if (maxentry != null && maxentry.getValue() != null) {
				path.add(maxentry);
				maxentry.getValue().findMostHittable(path);
			}
		}
		*/
	}
	
	public static Node initializeTrie() {
		return new Node();
	}
	
	public static boolean insertWord(Node root, String word) {
		return root.insert(word);
	}
	
	public static List<Pair<String, Long>> findMostHittable(Node root, int num) {
		List<Pair<String, Long>> result = new ArrayList<Pair<String, Long>>();
		root.findMostHittable("", num, result);
		return result;
	}
	
	public static List<Tuple3<String, Long, Long>> gatherPrefixes(Node root, int max) {
		List<Tuple3<String, Long, Long>> result = new ArrayList<Tuple3<String, Long, Long>>();
		root.gatherPrefixes(max, "", result);
		return result;
	}
	
	/*
	public static Pair<String, Integer> findMostHittable(Node root) {
		List<Map.Entry<String, Node>> path = new ArrayList<Map.Entry<String, Node>>();
		return findMostHittable(root, path);
	}
	
	public static Pair<String, Integer> findMostHittable(Node root, List<Map.Entry<String, Node>> path) {
		root.findMostHittable(path);
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Node> e : path) {
			sb.append(e.getKey());
		}
		return new Pair<String, Integer>(sb.toString(), path.get(path.size() - 1).getValue().hits);
	}
	*/
}
