package org.aksw.simba.quetsal.datastructues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Trie data structure
 * @author Saleem
 *
 */
public class Trie {
	static Logger log = LoggerFactory.getLogger(Trie.class);
	
	//static int max=-1;
	//static TrieNode maxNode = null;
	//static String lcp = "";
	//static boolean isPrefixedFound = false;
	//public static List<String> paths = new ArrayList<String>();
	
	/**
	 * Initialize Trie
	 * @return Root node
	 */
	static TrieNode initializeTrie() {
		return (new TrieNode());
	}
	/**
	 * Insert word into the Trie
	 * @param root root node
	 * @param word Word to be inserted
	 * @param branchLimit Branching limit
	 */
	static void insertWord(TrieNode root, String word,int branchLimit) {
		//	    	dfs(root,0);
		//	    	System.out.println(word);
		int l = word.length();
		char[] letters = word.toCharArray();
		TrieNode curNode = root;
		for (int i = 0; i < l; i++) {
			//System.out.println(letters[i]);
			if(curNode.children.size()<=branchLimit && curNode.insertFlag==true )
			{
				if (!curNode.children.containsKey(letters[i]))
					curNode.children.put(letters[i], new TrieNode(letters[i],curNode));

				if(curNode.children.size()>branchLimit)
				{
					curNode.insertFlag=false;
					curNode.children.clear();
				}
				else
					curNode = curNode.children.get(letters[i]);
			}
			//	        	else
			//	        	{
			//	        		curNode.insertFlag=false;
			//	        		curNode.children.clear();
			//	        		//curNode.children.clear();
			//	        	}


		}
	}
	/**
	 * Get the words in the Trie with the given
	 * prefix
	 * @param prefix Prefix
	 * @return a List containing String objects containing the words in
	 *         the Trie with the given prefix.
	 */
	public static TrieNode getLastNodeOfWord(String prefix, TrieNode root)
	{ //System.out.println(prefix);
		//Find the node which represents the last letter of the prefix
		TrieNode lastNode = root;
		for (int i = 0; i < prefix.length(); i++)
		{
			// TrieNode prevNode = lastNode;
			lastNode = lastNode.children.get(prefix.charAt(i));
			// System.out.println("chracter: "+ prefix.charAt(i) + "Node:" + lastNode.letter);
			//If no node matches, then no words exist, return empty list
			//  if (lastNode == null) return prevNode;      
		}
		return lastNode;

		//Return the words which eminate from the last node
		// return lastNode.getWords();
	}

	public static void main(String[] args) {
		//	int branchLimit =1;

		//	    ArrayList<String> words = new ArrayList<String>();
		//           words.add("http://aksw.org/resource/tcga-ab-4314/m-145");
		//           words.add("http://aksw.org/resource/tcga-ab-4314/m-2");
		//           words.add("http://aksw.org/resource/tcga-ab-4314/m-321");
		//           words.add("http://aksw.org/resource/tcga-ab-4315/m-1");
		//           words.add("http://aksw.org/resource/tcga-ab-4315/m-2");
		//		  constructTrie(words,branchLimit);
		//		  // System.out.println(curNode.letter);
		//		// List<String> uRIAllCombinations = getAllCombinations("http://aksw.org/resource/tcga-ab-4314/m-");
		//		 //System.out.println(uRIAllCombinations);
		//	  System.out.println( getRoot2StopingNodePaths(TrieNode.root,branchLimit));
		// words.clear();
		// words.add("http://sdow2008.semanticweb.org/");
		// words.add("http://sdow2008.semanticweb.org/resources/images/sdow.png");
		// words.add("chebi:31913");
		// words.add("chebi:41913");
		// constructTrie(words,branchLimit);
		// System.out.println( getRoot2StopingNodePaths(TrieNode.root,branchLimit));
		//  dfs(TrieNode.root,0);
		//	    Map<String, List<String>>  map = new HashMap<String,List<String>>();
		List<String> uris = new ArrayList<String>();
		uris.add("http://data.linkedmdb.org/resource/actor/");
		uris.add("http://data.linkedmdb.org/resource/");
		TrieNode troot = Trie.constructTrie(uris,30);
		System.out.println( getAllCombinations("http://data.linkedmdb.org/resource/", troot));
		//		    Trie.insertWord(root, "http://aksw.org/resource/pk124", branchLimit);
		//		    Trie.insertWord(root, "http://aksw.org/resource/pk125", branchLimit);
		//		    Trie.insertWord(root, "http://aksw.org/resource/pk126", branchLimit);
		// dfs(root,0);
		//   System.out.println( getRoot2StopingNodePaths(root,branchLimit));
		// dfs(root);
		// lcp(root);
		//System.out.println("LCP:"+lcp);
		//  rootToLeaves(root);
		// List<String> URIs = new ArrayList<String>();
		// URIs.add("http://aksw.org/resource/r0");
		// URIs.add("http://aksw.org/resource/r1");
		// URIs.add("http://deri.org/resource/r01");
		// URIs.add("http://deri.org/resource/r02");
		// Map<String, List<String>> authGroups=   createAuthGroups(URIs);

		//		    for(String auth:authGroups.keySet())
		//		    {
		//		    	List<String> uRIs = authGroups.get(auth);
		//		    	TrieNode rootNode = constructTrie(uRIs);
		//		    	String lcp = getLCP(rootNode);
		//		    	System.out.println("Authority: "+auth+", LCP:"+lcp);
		//		    	
		//		    }

		// ArrayList<String> lcps = Prefix.getLCPs(authGroups);
		// System.out.println(lcps);
	}
	

	
	/**
	 * Get all branching combinations of a Prefix
	 * @param uRI Prefix
	 * @return list of prefix combinations 
	 */
	public static List<String> getAllCombinations(String uri, TrieNode root) {
		List<String> combinations = new ArrayList<String> ();
		TrieNode curNode =  getLastNodeOfWord(uri, root);
		//System.out.println(curNode.letter);
		List<String>  paths = getRoot2StopingNodePaths(curNode,100);
		//System.out.println(paths);
		if(paths.size()>0)
		{
			for(String path:paths)
				combinations.add(uri + path);
		}
		//combinations.add(uRI.substring(0,uRI.length()-1)+path);
		return combinations;
	}
	
	/**
	 * Get all paths from root to marked nodes 
	 * @param root Roote note
	 * @param branchLimit Branchign Limit
	 * @return set of strings
	 */
	public static List<String> getRoot2StopingNodePaths (TrieNode root, int branchLimit) {
		// assume root != NULL
		List<String> paths = new ArrayList<String>();
		getRoot2StopingNodePaths(root, paths, new LinkedList<TrieNode>(), branchLimit);
		return paths;
	}
	
	/**
	 * Get all paths from root to marked stoping nodes
	 * @param root Roote node
	 * @param path Path
	 * @param branchLimit Branching Limit
	 */
	private static void getRoot2StopingNodePaths(TrieNode root, List<String> paths, LinkedList<TrieNode> path, int branchLimit)
	{
		path.add(root);
		if (root.children.isEmpty()) {
			String curPath = "" ;
			for(int node = 1; node<path.size(); node++)
			{
				if(path.get(node).parent.children.size()<=branchLimit)
				{
					curPath = curPath.concat(Character.toString(path.get(node).letter));
					//System.out.println(path.get(node).letter+"  Parent:"+path.get(node).parent.letter+" Parent Branches: "+path.get(node).parent.children.size());
				} else {
					curPath += "";
				}
			}
			// System.out.println("\n\n");
			if(!paths.contains(curPath))
				paths.add(curPath);
		} else {
			for (TrieNode node: root.children.values()) {
				getRoot2StopingNodePaths(node, paths, new LinkedList<TrieNode>(path),branchLimit);
			}
		}
	}

	/**
	 * Get logest common prefixes. Note part of QUETZAL
	 * @param authGroups Set of URIs
	 * @param branchLimit Branching limt 
	 * @return Logest common prefixes
	 */
	public static ArrayList<String> getLCPs(Map<String, List<String>> authGroups, int branchLimit) {
		ArrayList<String> lcps = new ArrayList<String>();
		for(String auth:authGroups.keySet())
		{
			List<String> uRIs = authGroups.get(auth);
			System.out.println(uRIs);
			TrieNode rootNode = constructTrie(uRIs, branchLimit);
			lcps.add(getLCP(rootNode));
			//	lcps.add(Prefix.longestCommonPrefix(uRIs));
			//System.out.println("Authority: "+auth+", LCP:"+lcp);

		}
		return lcps;
	}
	
	/**
	 * Construct Trie
	 * @param uRIs set of URIs
	 * @param branchLimit Branching limit
	 * @return Root node of Trie
	 */
	public static TrieNode constructTrie(Collection<String> uRIs, int branchLimit) {
		TrieNode root = Trie.initializeTrie();
		for (String uRI : uRIs)
			Trie.insertWord(root, uRI, branchLimit);

		return root;
	}
	
	/**
	 * Given set of URIs. Group them by same authorities
	 * @param URIs Set of URIs
	 * @return groups of URIs
	 */
	public static Map<String, List<String>> createAuthGroups(List<String> URIs) {
		Map<String, List<String>> authGroups = new ConcurrentHashMap<String, List<String>>();
		for(String curSbj:URIs)
		{
			String[] sbjPrts = curSbj.split("/");
			if ((sbjPrts.length>1))
			{
				try{
					String sbjAuth =sbjPrts[0]+"//"+sbjPrts[2];
					if(authGroups.containsKey(sbjAuth))
					{
						List<String> URIs1 = authGroups.get(sbjAuth);
						synchronized (URIs1)
						{
							URIs1.add(curSbj);
						}
					}
					else
					{
						List<String> newURIs = new ArrayList<String>();	
						newURIs.add(curSbj);
						authGroups.put(sbjAuth, newURIs);
					}
				}
				catch (Exception e){
					System.err.println("Subject is not a valid URI. Subject authority ignored");
				}
			}
		}
		//System.out.println(authGroups);
		return authGroups;
	}
	
	public static class Prefix {
		boolean isPrefixedFound = false;
		String lcp = "";
		
		public void append(char c) {
			lcp += c;
		}
	}
	/**
	 * Depth First search in Trie
	 * @param node starting node
	 * @param depth Depth 
	 */
	static void dfs(TrieNode node, Prefix pfx, int depth) {
		if (node.children.size() > 1) {
			pfx.isPrefixedFound = true;
		}
		for (TrieNode tn : node.children.values())
		{
			if (log.isDebugEnabled()) {
				log.debug("node: " + tn.letter + "  branches:" + tn.children.size() + " prent:" + tn.parent.letter);
			}
			if (!pfx.isPrefixedFound) {
				pfx.append(tn.letter);
			}
			dfs(tn, pfx, depth + 1);
		}
	}
	
	/**
	 * Get longest common prefix LCP
	 * @param node Node
	 * @return LCP
	 */
	static String getLCP(TrieNode node) {
		TrieNode first = node;
		String prefix = "";
		while (first.children.size()==1)
		{
			first = first.children.values().iterator().next();
			prefix = prefix + first.letter;
		}

		// System.out.println("found the first vertex on node:"+first.letter);
		//  System.out.println("The LCP is :"+prefix);
		//  System.out.println(first.children.size());
		return prefix;
	}
	
	/**
	 * Get longest common prefix
	 * @param strs Set of strings
	 * @return logest common prefix
	 */
	public String longestCommonPrefix(String[] strs) {
		String longestPrefix = "";
		if(strs.length>0){
			longestPrefix = strs[0];
		}
		for(int i=1; i<strs.length; i++){
			String analyzing = strs[i];
			int j=0;
			for(; j<Math.min(longestPrefix.length(), strs[i].length()); j++){
				if(longestPrefix.charAt(j) != analyzing.charAt(j)){
					break;
				}
			}
			longestPrefix = strs[i].substring(0, j);
		}
		return longestPrefix;
	}
	//	static void dfs(TrieNode node) {
	//	    // sanity check
	//	    if (node == null) {
	//	        return;
	//	    }
	//
	//	    // use a hash set to mark visited nodes
	//	    Set<TrieNode> set = new HashSet<TrieNode>();
	//
	//	    // use a stack to help depth-first traversal
	//	    Stack<TrieNode> stack = new Stack<TrieNode>();
	//	    stack.push(node);
	//
	//	    while (!stack.isEmpty()) {
	//	    	TrieNode curr = stack.pop();
	//
	//	        // current node has not been visited yet
	//	        if (!set.contains(curr)) {
	//	            // visit the node
	//	            // ...
	//               System.out.println(curr.letter);
	//	            // mark it as visited
	//	            set.add(curr);
	//	        }
	//
	//	        for (int i = 0; i < curr.children.size(); i++) {
	//	        	TrieNode neighbor = curr.children.get(i);
	//
	//	            // this neighbor has not been visited yet
	//	            if (!set.contains(neighbor)) {
	//	                stack.push(neighbor);
	//	            }
	//	        }
	//	    }
	//	}
}
