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
		long terminatorHits = 0;
		Map<String, Node> children = new HashMap<String, Node>();
		
		private Node() {
			
		}
		
		// node has termination
		private Node(long hits) {
			this.unique = 1;
			this.hits = hits;
			this.terminatorHits = hits;
			//children.put(tname, null);
		}
		
		private void insertPair(String k, Node v) {
			children.put(k, v);
		}
		
		public boolean insert(String head, String tail, long hs) {
			hits += hs;
			Node cn = children.get(head);
			if (cn == null) {
				cn = new Node();
				children.put(head, cn);
			}
			return cn.insert(tail, hs);
		}
		
		public boolean insert(String word, long hs) {
			boolean result = false;
			hits += hs;
			boolean childFound = false;
			if (children == null) {
				childFound = false;
			}
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				int i = 0;
				String k = e.getKey();
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
							result = curChild.insert(tail1, hs);
						} else {
							curChild.hits += hs;
							curChild.terminatorHits += hs;
						}
					} else {
						String commonPrefix = k.substring(0, i);
						String tail0 = k.substring(i);
						
						children.remove(k);
						Node newChild = new Node();
						newChild.hits = curChild.hits + hs;
						newChild.unique = curChild.unique + 1;
						children.put(commonPrefix, newChild);
						newChild.insertPair(tail0, curChild);
						newChild.insertPair(tail1, new Node(hs));
						result = true;
					}
					break;
				}
			}
			if (!childFound) { // no common prefix => new root
				insertPair(word, new Node(hs));// terminator mark
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
			if (terminatorHits > 0) {
				updateList(path, terminatorHits, maxsize, result);
			}
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				Node nd = e.getValue();
				if (result.size() < maxsize || result.get(result.size() - 1).getSecond() < nd.hits) {
					nd.findMostHittable(path + e.getKey(), maxsize, result);
				}
			}
		}
		
		public void gatherPrefixes(int branchLimit, List<Tuple3<String, Long, Long>> result)
		{
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				e.getValue().gatherPrefixes(branchLimit, e.getKey(), result);
			}
		}
		
		public void gatherPrefixes(int branchLimit, String curPrefix, List<Tuple3<String, Long, Long>> result)
		{
			if (terminatorHits > 0) {
				result.add(new Tuple3<String, Long, Long>(curPrefix, (long)1, terminatorHits));
			}
			int bcount = children.size();
			if (children.containsKey("")) {
				--bcount;
			}
			boolean stopFlag = bcount > branchLimit;
			for (Map.Entry<String, Node> e : children.entrySet())
			{
				Node nd = e.getValue();
				if (stopFlag) {
					result.add(new Tuple3<String, Long, Long>(curPrefix + e.getKey(), nd.unique, nd.hits));
				} else {
					nd.gatherPrefixes(branchLimit, curPrefix + e.getKey(), result);
				}
			}
		}
	}
	
	public static Node initializeTrie() {
		return new Node();
	}
	
	public static boolean insertWord(Node root, String head, String tail, long hits) {
		return root.insert(head, tail, hits);
	}
	
	//public static boolean insertWord(Node root, String word) {
	//	return root.insert(word);
	//}
	
	public static List<Pair<String, Long>> findMostHittable(Node root, int num) {
		List<Pair<String, Long>> result = new ArrayList<Pair<String, Long>>();
		root.findMostHittable("", num, result);
		return result;
	}
	
	public static List<Tuple3<String, Long, Long>> gatherPrefixes(Node root, int branchLimit) {
		List<Tuple3<String, Long, Long>> result = new ArrayList<Tuple3<String, Long, Long>>();
		root.gatherPrefixes(branchLimit, result);
		return result;
	}
}
