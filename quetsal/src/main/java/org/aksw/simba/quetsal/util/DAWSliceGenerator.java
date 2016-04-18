                                                                                                          
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.aksw.simba.quetsal.util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Slice Generator 
 * @author ngonga, saleem
 */
public class DAWSliceGenerator {

    /**
     * Generates the size of the knowledge bases
     *
     * @param totalSize Total size of input knowledge base
     * @param discrepancy Maximal size difference between two slices
     * @param numberOfSlices Number of slices to be generated
     * @return Distribution of slice sizes
     */
    public static List<Integer> getSliceSizes(int totalSize, int discrepancy, int numberOfSlices) {
        List<Double> delta = new ArrayList<Double>();
        delta.add(0.0);
        for (int i = 1; i < numberOfSlices - 1; i++) {
            delta.add(Math.random());
        }
        delta.add(1.0);

        double m = totalSize;
        for (int i = 0; i < delta.size(); i++) {
            m = m - discrepancy * delta.get(i);
        }

        m = Math.ceil(m / (double) numberOfSlices);
        List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < numberOfSlices; i++) {
            result.add((int) Math.ceil(m + delta.get(i) * (double) discrepancy));
        }
        return result;
    }

    /** write duplicate-slices from the input knowledge base according to the distribution in slices
     * 
     * @param sizes Size of the slices to be created
     * @param inputFile Input knowledge base/data dump
     * @param outputDir Output directory location for storing slicing
     * @return sliceSet
     */
    public static List<String> writeDuplicateFreeSlices(List<Integer> sizes, String inputFile, String outputDir) {
        List<String> slicesSet = new ArrayList<String>();
        try {
            String triple;
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
       	    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
       	   // saleem changes to avoid buffer, memory overflow
         //   BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            for (int i = 0; i < sizes.size(); i++) {
               // List<String> slice = new ArrayList<String>();
            	String sliceName = outputDir+(int)(i+1) +".nt";
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(sliceName)));
                 
                int size = sizes.get(i);
                for (int j = 0; j < size; j++) {
                    triple = reader.readLine();
                    if (triple != null) {
                       // slice.add(s);
                    	bw.write(triple);
                    	bw.newLine();
                    }
                }
                slicesSet.add(sliceName);
                bw.close();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return slicesSet;
    }

    /** Adds duplicates to the slices
     * 
     * @param slicesSet Slices without duplicates
     * @param numberOfDuplicates Number of slices to be used for generating duplicates
     * @return Slices with duplicates
     * @throws IOException 
     */   
    public static void addDuplicates(List<String> slicesSet, int numberOfDuplicates) throws IOException {
        if(numberOfDuplicates != 0) 
        {
        Set<Integer> duplicateIndex = new HashSet<Integer>();
        while (duplicateIndex.size() < numberOfDuplicates) {
            duplicateIndex.add((int) (slicesSet.size() * Math.random()));
        }
        System.out.println("Slice no. to be duplicated: "+duplicateIndex);
        List<List<String>> toBeAdded = new ArrayList<List<String>>();
        for (int i = 0; i < slicesSet.size(); i++) {
            toBeAdded.add(new ArrayList<String>());
        }
        for (int i : duplicateIndex) {
            //get data to be duplicated
            String duplicateSlice = slicesSet.get(i);
            //add data to other sources
            FileInputStream fstream = new FileInputStream(duplicateSlice);
            DataInputStream in = new DataInputStream(fstream);
       	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
           String triple;
       	 while ((triple= br.readLine()) != null)
    	  {
    	     int index;
                //pick a data source(slice) that is not one of the slice to be duplicated
                do {
                    index = (int) (slicesSet.size() * Math.random());
                } while (duplicateIndex.contains(index));
                // add data 
                //System.out.println("Add data to "+index);
                toBeAdded.get(index).add(triple);
            }
       	 br.close();
        }
        
        //merge
        int size = slicesSet.size();
       
        for(int sliceNo=0; sliceNo<size; sliceNo++)
        {
         	String sliceName = slicesSet.get(sliceNo);
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(sliceName),true)); //mind true. i.e we need to append the duplicated triple in the end of the file
           	for(int tripleNo=0; tripleNo<toBeAdded.get(sliceNo).size(); tripleNo++)
            {
               bw.write(toBeAdded.get(sliceNo).get(tripleNo));
               bw.newLine();
            }
           	bw.close();
        }
        System.out.println("Created duplicates");
        
        }
    }

     /**
     * Counts the number of lines in a file
     *
     * @param file Input file
     * @return Number of lines
     */
    public static int getFileSize(String file) {
        int count = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String s;
            s = reader.readLine();
            while (s != null) {
                s = reader.readLine();
                count++;
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return count;
    }

    /** Computes slices and writes them to the harddrive
     * 
     * @param input Input knowledge base
     * @param numberOfSlices Number of slices
     * @param discrepancy Value for the discrepancy. If the value is smaller than 1, then the discrepancy is assumed to express the fragment of the 
     * total size of the input KB that is to be used as discrepancy. Thus, 0.2 leads to the maximal discrepancy between slices being of 20% of the
     * size of the original KB.
     * @param numberOfDuplicateKBs Number of slices to duplicate
     * @param output Output files
     * @throws IOException 
     */
    public static void computeSlices(String input, int numberOfSlices, double discrepancy, int numberOfDuplicateKBs, String output) throws IOException {
        int kbSize = getFileSize(input);
        System.out.println("Found " + kbSize + " triples");
        int d = (int)discrepancy;
        if(discrepancy < 1)
        {
            d = (int)(discrepancy * (double) kbSize);
            System.out.println("Absolute value for discrepancy is "+d);
        }
            List<Integer> sliceSizes = getSliceSizes(kbSize, d, numberOfSlices);
        System.out.println("Slices size are " + sliceSizes);
        List<String> slicesSet = writeDuplicateFreeSlices(sliceSizes, input,output);
        System.out.println("Wrote duplicate-free slices");
        addDuplicates(slicesSet, numberOfDuplicateKBs);
        System.out.println("Wrote slices to " + output);
    }

     /** Returns the parameters for the tool
     * 
     */
    public static void usage()
    {
        System.out.println("\nPlease give in the parameters in the following order:");
        System.out.println("arg1: Source file (only nt supported)");
        System.out.println("arg2: Number of slices (integer)");
        System.out.println("arg3: Discrepancy (absolute value, integer)");
        System.out.println("arg4: Number of slices to duplicate (integer)");
        System.out.println("arg5: Pattern for output file");        
    }
    
    public static void main(String args[]) throws IOException {
      //  if(args.length != 5)
        //    usage();
        //else
    	String dataSet="C:/Users/muhsal/Desktop/FedBench/Benchmark2/DBPedia Subset/dbpedia-ntripples/dbpedia_subset.nt";
    	int noOfSlices= 10;
    	int discrepancy=280000;
    	int noDupSlices=0;
    	String outputFolder="C:/Users/muhsal/Desktop/FedBench/Benchmark2/DBPedia Subset/dbpedia-ntripples/slices/dbpedia";
    	//System.out.println("Press Any Key to Start ...");  
    	//JucDemoSingleQuery.waitForKey();
    	    computeSlices(dataSet,noOfSlices,discrepancy,noDupSlices,outputFolder);
        //    ServiceDescription sd = new ServiceDescription();
         //   sd.main(null);
           // JucDemoSingleQuery demo = new JucDemoSingleQuery();
            //demo.main(args);
            
    }
}
