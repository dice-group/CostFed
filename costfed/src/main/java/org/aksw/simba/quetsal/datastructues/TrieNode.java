package org.aksw.simba.quetsal.datastructues;

import java.util.HashMap;
import java.util.Map;
/**
 * Trie Node implementation
 * @author Saleem
 *
 */
public class TrieNode {
	char letter;
	Map<Character,TrieNode> children;
	TrieNode parent;
	boolean insertFlag = true ;  // if true then child can be inserted
	TrieNode root;

	TrieNode(char letter, TrieNode parentNode) {
        this.letter = letter;
        this.children = new  HashMap<Character, TrieNode>();
        this.parent = parentNode;
        this.insertFlag = true;
        this.root = parentNode.root;
    }
	
    /**
     * Constructor
     */
    TrieNode() {
        this.letter = '\0';
        this.children = new HashMap<Character, TrieNode>();
        this.parent = this;
        this.root = this;
        this.insertFlag = true;
    }
}