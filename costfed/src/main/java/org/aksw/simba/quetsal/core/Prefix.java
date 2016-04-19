package org.aksw.simba.quetsal.core;
/**
 * Calculate longest common prefix of URIs. This class is not part of QUETZAL
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Prefix {

	public static void main(String[] args) {

	}
	/**
	 * Logest common prefix of URIs
	 * @param uRIs Set of URIs
	 * @return logest common prefix
	 */
	public static String longestCommonPrefix(List<String> uRIs) {
		if (uRIs.size() == 0) {
			return "";   // Or maybe return null?
		}

		for (int prefixLen = 0; prefixLen < uRIs.get(0).length(); prefixLen++) {
			char c = uRIs.get(0).charAt(prefixLen);
			for (int i = 1; i < uRIs.size(); i++) {
				if ( prefixLen >= uRIs.get(i).length() ||
						uRIs.get(i).charAt(prefixLen) != c ) {
					// Mismatch found
					return uRIs.get(i).substring(0, prefixLen);
				}
			}
		}
		return uRIs.get(0);
	}
	public static ArrayList<String> getLCPs(Map<String, List<String>> authGroups) {
		ArrayList<String> lcps = new ArrayList<String>();
		for(String auth:authGroups.keySet())
		{
			List<String> uRIs = authGroups.get(auth);
			lcps.add(Prefix.longestCommonPrefix(uRIs));
			//System.out.println("Authority: "+auth+", LCP:"+lcp);

		}
		return lcps;
	}
}
