package org.aksw.simba.quetsal.core;
//package org.aksw.simba.quetzal.core;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
//import org.aksw.simba.quetsal.synopsis.MIPsynopsis;
//
//import com.hp.hpl.jena.graph.Triple;
//
///**
// * Single Triple Pattern Source Ranking and Skipping
// * @author saleem
// *
// */
public class DAWTriplePatternRanking {
///**
// * Single Triple pattern shource ranking and Skipping. 
// * Note that the selectivity multiplication to the MIPs vectors are pre computed. 
// * @param starting_SrviceUrl endpoint Url having max initial results estimated
// * @param rsCount Number of initial estimated results
// * @param hshMipsV Hash map of all the MIPs Vectors for the current triple pattern i.e trpl
// * @param trpl Tripl Pattern
// * @param threshold Threshold value used for source skipping
// */
//public void MipsSynBased_STPRanking(String starting_SrviceUrl, Long rsCount, HashMap<String, MIPsynopsis> hshMipsV, Triple trpl, Long threshold )
//	{
//		System.out.println("----MIPS Synopsis based Source Selection Ranking----===================");
//		System.out.println("1. Selected Service Url: " + starting_SrviceUrl+" Total Estimated Records:\t" + rsCount);
//		MIPsynopsis curMIPsVector = hshMipsV.get(starting_SrviceUrl);
//		MIPsynopsis selectedMIPsVector = curMIPsVector; 
//		MIPsynopsis UnionVector = selectedMIPsVector;
//		String selectedSrviceURL=starting_SrviceUrl;
//		hshMipsV.remove(starting_SrviceUrl);
//		int count = 1;
//		long MaxdistinctRecords=-1;
//		String SrviceURL="";
//		do
//		{
//			Iterator ServicesIterator = hshMipsV.entrySet().iterator();
//			MaxdistinctRecords=-1;
//			while (ServicesIterator.hasNext())  //while 1 started
//			{
//				Map.Entry entry = (Map.Entry) ServicesIterator.next();
//				SrviceURL = (String) entry.getKey();
//				curMIPsVector = hshMipsV.get(SrviceURL);
//				// Collection CurVector = new ArrayCollection(curIdsVector);
//				long curOverlap= UnionVector.intersectionSize(curMIPsVector);
//				long curDistinctRecords = curMIPsVector.getOriginalSize()-curOverlap;
//				if(curDistinctRecords>MaxdistinctRecords)
//				{
//					selectedMIPsVector = curMIPsVector;
//					selectedSrviceURL=SrviceURL;
//					MaxdistinctRecords=curDistinctRecords;
//				}
//			}
//			if(MaxdistinctRecords>threshold)
//				UnionVector =  (MIPsynopsis) UnionVector.union(selectedMIPsVector);
//			else
//				selectedSrviceURL=SrviceURL;
//			count++;
//			System.out.println(count+". Selected Service Url: "+selectedSrviceURL + " Total Estimated Records:\t"+ UnionVector.getOriginalSize() );
//			hshMipsV.remove(selectedSrviceURL);
//	
//		}
//		while(hshMipsV.size()>0);	
//		}
}
