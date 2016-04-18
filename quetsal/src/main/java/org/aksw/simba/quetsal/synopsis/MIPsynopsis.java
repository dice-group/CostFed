package org.aksw.simba.quetsal.synopsis;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

import org.aksw.simba.quetsal.synopsis.ArrayCollection;
import org.aksw.simba.quetsal.synopsis.Collection;


/*
 * Created on 16.05.2005 by Sebastian Michel
 * Max-Planck Institute for Computer Science
 * 
 * smichel@mpi-sb.mpg.de
 *
 */

/**
 * 
 * 
 *
 * @author Sebastian Michel, MPII, smichel@mpi-sb.mpg.de
 *
 */ 
public class MIPsynopsis  extends Synopsis implements Serializable{

	private static final long serialVersionUID = 3977577013362047028L;
	   
    
   public long[] minValues;
    private IndependentPermutations permut = null;
    public long originalSize;
    private long seed;
   
    public MIPsynopsis(int numberOfPermutations, long seed) {
        this.minValues = new long[numberOfPermutations];
        this.permut = new IndependentPermutations(numberOfPermutations, seed);
    }
    
    public MIPsynopsis(Collection c, int numberOfPermutations, long seed) {
        this(numberOfPermutations, seed);
        this.seed = seed;
        this.originalSize = c.size();

        //one scan over all documents
        
        for (int i=0; i<c.size(); i++) {
            for (int p=0; p<numberOfPermutations; p++) {
                long temp = permut.apply(p, c.getDocByRank(i));
                minValues[p] = Math.min(minValues[p], temp);
                
            }
        }

    }
    
    public MIPsynopsis(String[] strMIPsV, long seed,long origSize) 
    {
    	 // this(strMIPsV.length, seed);
    	  this.seed = seed;
          this.originalSize = origSize;
          this.minValues = new long[strMIPsV.length];
         // System.out.println(strMIPsV.length);
          for (int p=0; p<strMIPsV.length; p++) 
          {
        	   minValues[p] = Long.parseLong(strMIPsV[p]);
          }
 	  }

	/* (non-Javadoc)
     * @see Synopsis#overlap(Synopsis)
     */
    public double resemblance(Synopsis s) {
        MIPsynopsis s_mip = (MIPsynopsis) s;
        
        int minSize = Math.min(this.minValues.length, s_mip.minValues.length);
        
        int count=0;
        for (int i=0; i<minSize; i++) {
            
            if ((this.minValues[i]!=-1)&&(this.minValues[i]==s_mip.minValues[i])) count++;
            
        }
        
        return count/(double) minSize;
    }

    /* (non-Javadoc)
     * @see Synopsis#novelty(Synopsis)
     */
   public double noveltyWithSynopsis(Synopsis s) {  //this is already retrieved set, s = candidate set synopsis
        double resemblance = resemblance(s);
        double schnitt = (int) Math.ceil(resemblance *(s.getOriginalSize()+this.originalSize) / (resemblance+1));
        double ret = s.getOriginalSize()-schnitt;
        if (ret<0) return 0;   
        return ret;
    }

   /*
    * JOSI's code
    */
   public int intersectionSize(Synopsis s){
	   double resemblance = resemblance(s);
	   int intersectionSize = (int) Math.ceil(resemblance *(s.getOriginalSize()+this.originalSize) / (resemblance+1));
      // System.out.println("Intersection Size: " + intersectionSize);
	   return intersectionSize;
	   
   }
   
   
   
    /* (non-Javadoc)
     * @see overlap.Synopsis#getOriginalSize()
     */
    public long getOriginalSize() {

        return originalSize;
    }

    /* (non-Javadoc)
     * @see overlap.Synopsis#union(overlap.Synopsis)
     */
    public Synopsis union(Synopsis s) {

        MIPsynopsis s_ = (MIPsynopsis) s;
        int maxSize = Math.max(this.minValues.length, s_.minValues.length);
        MIPsynopsis new_s = new MIPsynopsis(maxSize, this.seed);

        double resemblance = resemblance(s_);
        
        new_s.originalSize = (int) Math.ceil((s_.originalSize+this.originalSize) / (resemblance+1));
        
        for (int i=0; i<maxSize; i++) {
            long x1 = (this.minValues.length-1<i)?Long.MAX_VALUE:this.minValues[i];
            long x2 = (s_.minValues.length-1<i)?Long.MAX_VALUE:s_.minValues[i];
            
            if (x1==-1) new_s.minValues[i] = x2;
            else if (x2==-1) new_s.minValues[i] = x1;
            else new_s.minValues[i] = Math.min(x1, x2);
        }
        return new_s;
    }

    /* (non-Javadoc)
     * @see overlap.Synopsis#intersect(overlap.Synopsis)
     */
    public Synopsis intersect(Synopsis s) {
        MIPsynopsis s_ = (MIPsynopsis) s;
        int minSize = Math.min(this.minValues.length, s_.minValues.length);
        MIPsynopsis new_s = new MIPsynopsis(minSize, this.seed);

        double resemblance = resemblance(s_);
        new_s.originalSize = (int) Math.ceil(resemblance *(s_.originalSize+this.originalSize) / (resemblance+1));

        for (int i=0; i<minSize; i++) {
            if ((this.minValues[i]!=-1)&&(this.minValues[i]==s_.minValues[i])) { //take it
                new_s.minValues[i] = this.minValues[i];
            }
            else {  //dummy element indicating that there was a mismatch
                new_s.minValues[i] = -1;//Math.min(this.minValues[i], s_.minValues[i]);
            }
        }
        return new_s;
        //throw new UnsupportedOperationException("if you know how to calculate this, please let us know ........");
    }

    public double containment(MIPsynopsis syn){
    	
    	return ((double) (this.intersect(syn)).getOriginalSize())/ ((double) this.getOriginalSize());
    	
    }
    
    	public static void main(String[] args) {
    	    //try {
    	       /* String [] strArr1 = new String []{"msaleem","josi","manfred","vinod","danh"};
    	        String [] strArr2 = new String []{"anh","vinod","josi","whoan","mesaleem"};
    	        String [] strArr3 = new String []{"anh","danh","whoan","xyz","cdf"};
    	        long[] set3 = new long[strArr1.length];
    	        long[] set4 = new long[strArr2.length];
    	        long[] set5 = new long[strArr3.length];*/
    	        //-----------------------DS1T1----rdf:type--------------------------------
    	        String strDS1T1="1014299725 -914855894 -1212838365 -1480720788 -1755948158 988904870 -1252077091 443805499 -1230092545 1274691891 1174166187 -1340770491 -158134976 807296218 -78936286 1931817357 -811836799 -1017432 1190633355 555611048 -1689968406 1938110317 -575329469 -2125960631 -1250097757 -180951308 485533725 313011348 927781941 -1004574415 -1464117570 686215461 -1741290212 -2110816502 -1478031539 -1920909434 -808904406 -1850397198 228635138 -1055150905 1427022939 1960890459 579960746 -1572018846 1721590016 -198669470 1930216586 1139470320 -387421230 -1526331471 1834505176 2053713729 354761537 605444797 -193768297 -647550529 -354831008 -179935908 -326972185 1472980869 -180108794 1076329607 -718818822 748223"; 
    	        String[] arrStrDS1T1 = strDS1T1.split(" ");
    	        long[] DS1T1 = new long [arrStrDS1T1.length] ;
    	        for(int i=0;i<arrStrDS1T1.length;i++)
    	        {
    	        	DS1T1[i]= Long.parseLong(arrStrDS1T1[i]);
    	        	//System.out.println(DS1T1[i]);
    	        }
    	        //-----------------------DS1T2---------rdfs:label---------------------------
    	        String strDS1T2="-959814900 -1188520484 1812172151 -1700581647 1898881551 -969282392 -2030053780 1194930720 79942196 -598163948 -684170938 -1130798271 635958980 -1261802744 -1817793288 2111488827 557301170 1693054461 843995129 -1127618716 1743878598 1061550005 -1895081662 -2060253606 1768485678 1689446924 -982927554 -470727154 748305842 590004368 -1020443359 1146143714 -1692321776 1953500791 1766652112 -476734792 846534961 -123376956 1647106273 -1018558670 -1462751486 1512354280";
    	        String[] arrStrDS1T2 = strDS1T2.split(" ");
    	        long[] DS1T2 = new long [arrStrDS1T2.length] ;
    	        for(int i=0;i<arrStrDS1T2.length;i++)
    	        {
    	        	DS1T2[i]= Long.parseLong(arrStrDS1T2[i]);
    	        }
    	        //-----------------------DS1T3-----<http://www.georss.org/georss/point>-----------------------
    	        String strDS1T3="-1654881284 -1319995246 -708938089 -1536757729 1613266065 -1382622894 2040342404 -358826630 1063106379 401111501 -2084548718 510732895 860423496 795273892 -880981305 -1045895421 -1548359684 -1361807457 -2027127369 -1447696033 749606349 -1349591360 -1409362731 -985746167 -32566255 1453162870 -637924922 2043375996 873608055 -741061939 -795103083 1588676052 41823168 1813396932 1329647809 -635535 920448720 1556936163 1097095737 -569313216 -1324736363 -74012962 507113439 1431980151 -553314486 1985364158 -984797527 1086497643 1579091770 -654096373 -1394596822 -969246129 35444962 908170246 1285938502 1588927319 946202190 1096193102 452591298 -107341007 -1573620049 -1236686926 302914815 -1285068420";
    	        String[] arrStrDS1T3 = strDS1T3.split(" ");
    	        long[] DS1T3 = new long [arrStrDS1T3.length] ;
    	        for(int i=0;i<arrStrDS1T3.length;i++)
    	        {
    	        	DS1T3[i]= Long.parseLong(arrStrDS1T3[i]);
    	        } 
    	        //-----------------------DS1T4----------------geo:long--------------------
    	        String strDS1T4="974919841 -1647898283 -1727761445 846410993 -2119972003 1424657835 -1986982795 1646817194 1000417270 876546828 209448440 1687911577 -498942042 565949044 -1713674860 -1671816128 186184167 1773796685 861040055 -6812096 540661176 -50008840 1515955063 -1965864949 -688761430 -450884911 1417367197 2001991247 1321830913 905273022 1112799271 1033481210 -890374310 -1741153796 326642365 1791588786 1169995890 -922728995 -308655373 441195831 796525176 -460732418 1669191673 1233119305 -1220306896 -391686270 150926261 741925585 -1618196941 2134875125 500883042 798485256 1631931135 -727339878 113367077 -2079663209 -828390956 115385987 -1050636286 -960756597 408830717 -1609812607 -229806957 48669233";
    	        String[] arrStrDS1T4 = strDS1T4.split(" ");
    	        long[] DS1T4 = new long [arrStrDS1T4.length] ;
    	        for(int i=0;i<arrStrDS1T4.length;i++)
    	        {
    	        	DS1T4[i]= Long.parseLong(arrStrDS1T4[i]);
    	        } //-----------------------DS1T5------------------geo:lat------------------
    	        String strDS1T5="-1120112206 2009124049 1495795305 -2078883546 1695477200 -1313634750 1809551124 1527773323 1452623914 1157220314 -1087897994 -914804559 -1842496415 -227855136 1866612393 65752773 -1044590655 -876377478 -1039457530 1520770627 -914672201 -1399281903 -1927761116 -1240058280 1635914068 -639474011 399624430 929712172 1690416915 -1221007838 1487536426 -631910150 -2067650222 -54471698 1790754820 593726556 2079534952 659185585 1245148587 -1784555234 41350201 -1629612530 24565858 -1348943673 -2088298632 348192491 -1966255808 1589971990 -746444733 901722708 -1289477774 1378253479 -1615875201 -590826812 -847890598 -1650728398 1741506174 -115475882 -1612953497 1176726908 1646439046 173279529 1311240484 1666394203";
    	        String[] arrStrDS1T5 = strDS1T5.split(" ");
    	        long[] DS1T5 = new long [arrStrDS1T5.length] ;
    	        for(int i=0;i<arrStrDS1T5.length;i++)
    	        {
    	        	DS1T5[i]= Long.parseLong(arrStrDS1T5[i]);
    	        }
    	        //-----------------------DS1T6,DS1T7----<http://linkedgeodata.org/ontology/memberOfWay>, <http://linkedgeodata.org/ontology/fee>;--------------------------------
    	      
    	        long[] DS1T6 = new long []{317063290, 550347877} ;
    	        long[] DS1T7 = new long []{-1579698332} ;
    	        
    	        //-----------------------------------------DATASET 2--------------------------------------------------------------------------
    	      //-----------------------DS2T1----------rdf:type--------------------------
    	        String strDS2T1="1014299725 -914855894 -1212838365 -1480720788 -1685767486 166507846 -52963487 1280841902 429727272 550481214 -324660247 1990962318 -158134976 676623122 1906669854 1931817357 -1979125241 907430382 555611048 -423718767 -1743933843 -1250097757 -780654773 1018256440 1335017964 1928883241 -1745879071 -1501260599 -1972967957 621229439 233693423 283247848 1619960650 -1735594451 836961565 1654515726 777551514 -885345605 474566270 1821337656 -231228653 1545276036 583115520 86235630 -969552255 -2067892984 1489785871 -356781809 427588008 1788954547 1536873346 -306190565 -1625025995 415117580 -39397019 -1170620282 -1717374441";
    	        String[] arrStrDS2T1 = strDS2T1.split(" ");
    	        long[] DS2T1 = new long [arrStrDS2T1.length] ;
    	        for(int i=0;i<arrStrDS2T1.length;i++)
    	        {
    	        	DS2T1[i]= Long.parseLong(arrStrDS2T1[i]);
    	        }
    	      //-----------------------DS2T2-------------------rdfs:label-----------------
    	        String strDS2T2="-959814900 -1188520484 1812172151 1898881551 -969282392 -2030053780 -2091822991 1183127309 -33215794 1852434938 -840956628 -509387268 -1587638130 -1740287725 1289719528 -744779453 1652320123 -1478360115 480752118 711258126 657800570 1119664772 1135625124 -1781683345 1818460855 1340065357 -146773897";
    	        String[] arrStrDS2T2 = strDS2T2.split(" ");
    	        long[] DS2T2 = new long [arrStrDS2T2.length] ;
    	        for(int i=0;i<arrStrDS2T2.length;i++)
    	        {
    	        	DS2T2[i]= Long.parseLong(arrStrDS2T2[i]);
    	        }
    	        //-----------------------DS2T3-------<http://www.georss.org/georss/point>-----------------------------
    	        String strDS2T3="-1654881284 -1319995246 -708938089 -1536757729 1613266065 -1382622894 2040342404 -358826630 1770347080 975229430 272695332 -1988761094 -1431112609 537649869 -833218728 930028740 636483034 -1048611599 -2048371371 164806506 -954756271 -1559894053 882404302 1692330796 820056214 -1553780990 2119614164 -977968948 486603943 -1469372297 -1908892093 -691327150 496958037 969915290 -890928161 728614308 -503217645 1739250386 1406946576 166691193 -373940781 -1017558990 -1932737097 -137064677 348883131 -1725816705 413996448 -223664341 457169583 -1207850714 248862825 1059712424 -462542700 1196375725 1868675748 162298916 -1532448979";
    	        String[] arrStrDS2T3 = strDS2T3.split(" ");
    	        long[] DS2T3 = new long [arrStrDS2T3.length] ;
    	        for(int i=0;i<arrStrDS2T3.length;i++)
    	        {
    	        	DS2T3[i]= Long.parseLong(arrStrDS2T3[i]);
    	        } 
    	        //-----------------------DS2T4---------geo:long---------------------------
    	        String strDS2T4="1393462568 1943024281 -1659619121 -262742375 1112136894 944453110 -940046158 -1377282583 1927345294 784223219 -620360088 1101536631 -1159496079 -97364492 164283375 511867495 158123223 -47672336 -6812096 -1477738926 540661176 1572600651 -2037597138 2001991247 294201692 521804006 -400305954 -1029831534 27538732 -336176934 1460363950 -689616574 1357760177 1433919507 158512028 -308655373 -1731605861 1233119305 -1220306896 -1064911629 241987818 1152261455 -1290473228 -218989761 261940238 1819683650 -760412553 795567769 -1384519414 9882684 -446935170 -165682486 -677929013 -498149941 -2079663209 -827857735 -229806957";
    	        String[] arrStrDS2T4 = strDS2T4.split(" ");
    	        long[] DS2T4 = new long [arrStrDS2T4.length] ;
    	        for(int i=0;i<arrStrDS2T4.length;i++)
    	        {
    	        	DS2T4[i]= Long.parseLong(arrStrDS2T4[i]);
    	        } 
    	        //-----------------------DS2T5---------geo:lat---------------------------
    	        String strDS2T5="-495994875 -354907735 85807589 655245565 -554550970 2059929340 -1329378219 -1087897994 -1842496415 -1885720985 -170983863 -374979788 -1054746182 65752773 339406401 -208365282 676176461 -1300049337 247618832 -1927761116 -410394123 40512415 -1240058280 -1593438639 -1231862265 -598353844 -1797282496 -314593152 777899351 -921194326 1572415659 -867462377 1586594096 423911825 40634541 1871149172 -1531198142 2132187660 1740499375 549205347 934404388 1565695993 -1572845537 895078501 -976482210 1074734307 170660100 -1458393719 -746444733 901722708 -1615875201 -52087721 609402303 1215543826 -1723783421 1640091522 1664248261";
    	        String[] arrStrDS2T5 = strDS2T5.split(" ");
    	        long[] DS2T5 = new long [arrStrDS2T5.length] ;
    	        for(int i=0;i<arrStrDS2T5.length;i++)
    	        {
    	        	DS2T5[i]= Long.parseLong(arrStrDS2T5[i]);
    	        }
    	        //-----------------------DS2T6------<http://linkedgeodata.org/ontology/memberOfWay> ------------------------------
    	        String strDS2T6="-998472433 633983938 -163849764 1479817555 2077694688 1300174666 1255177290 -1598938158 979008701 109564952 1652970181 1415632842 1415632681 -411833234 -2070464655";
    	        String[] arrStrDS2T6 = strDS2T6.split(" ");
    	        long[] DS2T6 = new long [arrStrDS2T6.length] ;
    	        for(int i=0;i<arrStrDS2T6.length;i++)
    	        {
    	        	DS2T6[i]= Long.parseLong(arrStrDS2T6[i]);
    	        }
    	      //----------------------DS2T7-------------<http://linkedgeodata.org/ontology/fee>-----------------------
      	      
    	         	        long[] DS2T7 = new long []{-689978262} ;
    	         	        
    	     //#############################################dataset 3####################################################################
    	         	       
    	         	        //-----------------------DS3T1----rdf:type--------------------------------
    	        	        String strDS3T1="-2111402227 -707316288 -2053138730 754460653 1067334160 -1171743034 -1734688663 -1921592617 1651754292 547932382 -491835402 -1899372893 -1855270429 1700513098 -1487081109 917997895 724013920 -1160420705 902479177 456877778 -2099184598 -148684826 1104289563 -1675249927 -1164343928 708495202 -469837754 931102478 844543325 -1047056122 704431475 -27328143 968266934 1794872514 1148332149 -179609146 549006745 -1265551521";
    	        	        String[] arrStrDS3T1 = strDS3T1.split(" ");
    	        	        long[] DS3T1 = new long [arrStrDS3T1.length] ;
    	        	        for(int i=0;i<arrStrDS3T1.length;i++)
    	        	        {
    	        	        	DS3T1[i]= Long.parseLong(arrStrDS3T1[i]);
    	        	        	//System.out.println(DS1T1[i]);
    	        	        }
    	        	        //-----------------------DS3T2---------<http://www.georss.org/georss/point>---------------------------
    	        	        String strDS3T2="-1313048898 -616551657 1919315426 118278391 200203243 431485631 1189870158 -1624078542 277973921 1558517811 243653464 2038291178 595797228 -1019498557 1249357499 896349971 1248891632 -1566996643 -376312391 738833833 -1582829633 -528440742 319189237 1318746012 1530595171 1255649272 836930987 -927759960 -1498564795 791055742 568499814 2057823872 1274335892 1980003763 722417865 843961996 1140539521 872229989";
    	        	        String[] arrStrDS3T2 = strDS3T2.split(" ");
    	        	        long[] DS3T2 = new long [arrStrDS3T2.length] ;
    	        	        for(int i=0;i<arrStrDS3T2.length;i++)
    	        	        {
    	        	        	DS3T2[i]= Long.parseLong(arrStrDS3T2[i]);
    	        	        }
    	        	        //-----------------------DS3T3------------geo:long----------------
    	        	        String strDS3T3="103621605 824261932 1434447688 -801948857 1408808091 2130698622 -1311344982 -23787459 480944821 715488554 -711164866 -695802225 1895008747 -292493624 1730101793 -144580957 59829007 895565919 -143093994 -1638090092 -1095994334 -290896604 1016862499 1119094184 -1223070967 69078671 1728765620 1612217084 1909216115 1179895488 -519628714 940647489 -1294215076 1702144532 1829347896 1383212698 151102849 709497664";
    	        	        String[] arrStrDS3T3 = strDS3T3.split(" ");
    	        	        long[] DS3T3 = new long [arrStrDS3T3.length] ;
    	        	        for(int i=0;i<arrStrDS3T3.length;i++)
    	        	        {
    	        	        	DS3T3[i]= Long.parseLong(arrStrDS3T3[i]);
    	        	        } 
    	        	        //-----------------------DS3T4--------------geo:lat----------------------
    	        	        String strDS3T4="2032091343 1812154697 1243744211 -418183550 96866411 1451313820 1903516630 1619179915 1530318163 1913948133 59223936 -1674850070 1793704762 1513711359 2012700211 -1601647268 -349738063 1667770404 386958837 1604622957 -1334166274 1805388257 -966421439 1028951148 1154569099 550854909 634640219 -1375408606 -1830140393 -476521989 1343325155 -820575841 -1992038237 -437129194 -1980169312 -865924349 1878358978 1515554374";
    	        	        String[] arrStrDS3T4 = strDS3T4.split(" ");
    	        	        long[] DS3T4 = new long [arrStrDS3T4.length] ;
    	        	        for(int i=0;i<arrStrDS3T4.length;i++)
    	        	        {
    	        	        	DS3T4[i]= Long.parseLong(arrStrDS3T4[i]);
    	        	        } 
    	        	        //-----------------------DS3T5-------<http://linkedgeodata.org/ontology/memberOfWay> -----------------------------
    	        	        String strDS3T5="-1481723889 -2134409024 -1307861278 -168336964 -1060738344 -1542085164 -142191953 -1617258572 -860744734 1864323714 1861574170 1637112126 78430372 78430371 1879896731 2094089897 1386233934 -1323464841 1329720594 1977469692 -1762325486 1788336738 -43837663 1929850198 -753693369 1129867133 1722191179 78217588 -725343163 -1138962459 -1154202418 1309835950 -563571927 -1561899980 1460444558 1352268600 1917609563 1627316169 -1922725722 2139442143 -2013182042 -272604695 -77821382 -78808329 -1440775147 1196554023 -1377244170 -45083177 -544665646 -597092008 533799962 1374325993 -1192534164 -199708588 -200663697 -202456142 -17305569 1084491909 -1570631483 -1673524064"; 
    	        	        String[] arrStrDS3T5 = strDS3T5.split(" ");
    	        	        long[] DS3T5 = new long [arrStrDS3T5.length] ;
    	        	        for(int i=0;i<arrStrDS3T3.length;i++)
    	        	        {
    	        	        	DS3T5[i]= Long.parseLong(arrStrDS3T5[i]);
    	        	        	//System.out.println(DS3T5[i]);
    	        	        }
    	        	       
    	        	        
    	        	       /* HashMap<Integer, long[]> HshPred = new HashMap();
    	        	        HshPred.put(1, DS3T5);
    	        	        HshPred.put(2, DS3T4);
    	        	       long[] arrPredFilter=  (long[]) HshPred.get(2);
    	        	       for(int j= 0; j< arrPredFilter.length;j++)
                     	  {
                     		 System.out.print(arrPredFilter[j]+ " "); 
                     	  }*/
    	        	        
    	     //----------------------------------------------------------------------------------------------------------------
    	         	   /* Collection colDS1T1 = new ArrayCollection(DS1T1);
    	         	    Collection colDS1T2 = new ArrayCollection(DS1T2);
    	         	    Collection colDS1T3 = new ArrayCollection(DS1T3);
    	         	    Collection colDS1T4 = new ArrayCollection(DS1T4);
    	         	    Collection colDS1T5 = new ArrayCollection(DS1T5);
    	         	    Collection colDS1T6 = new ArrayCollection(DS1T6);
    	         	    Collection colDS1T7 = new ArrayCollection(DS1T7);
    	         	 
    	         	    Collection colDS2T1 = new ArrayCollection(DS2T1);
   	         	        Collection colDS2T2 = new ArrayCollection(DS2T2);
   	         	        Collection colDS2T3 = new ArrayCollection(DS2T3);
   	         	        Collection colDS2T4 = new ArrayCollection(DS2T4);
   	         	        Collection colDS2T5 = new ArrayCollection(DS2T5);
   	         	        Collection colDS2T6 = new ArrayCollection(DS2T6);
   	         	        Collection colDS2T7 = new ArrayCollection(DS2T7);
   	         	        
   	         	        Collection colDS3T1 = new ArrayCollection(DS3T1);
	         	        Collection colDS3T2 = new ArrayCollection(DS3T2);
	         	        Collection colDS3T3 = new ArrayCollection(DS3T3);
	         	        Collection colDS3T4 = new ArrayCollection(DS3T4);
	         	        Collection colDS3T5 = new ArrayCollection(DS3T5);
	         	      
    	       	        
    	       	        MIPsynopsis synDS1T1 = new MIPsynopsis(colDS1T1, 1000, 242);
    	       	        MIPsynopsis synDS1T2 = new MIPsynopsis(colDS1T2, 1000, 242);
    	       	        MIPsynopsis synDS1T3 = new MIPsynopsis(colDS1T3, 1000, 242);
    	       	        MIPsynopsis synDS1T4 = new MIPsynopsis(colDS1T4, 1000, 242);
    	       	        MIPsynopsis synDS1T5 = new MIPsynopsis(colDS1T5, 1000, 242);
    	             	MIPsynopsis synDS1T6 = new MIPsynopsis(colDS1T6, 1000, 242);
    	       	        MIPsynopsis synDS1T7 = new MIPsynopsis(colDS1T7, 1000, 242);
    	       	   
    	       	        MIPsynopsis synDS2T1 = new MIPsynopsis(colDS2T1, 1000, 242);
 	       	            MIPsynopsis synDS2T2 = new MIPsynopsis(colDS2T2, 1000, 242);
 	       	            MIPsynopsis synDS2T3 = new MIPsynopsis(colDS2T3, 1000, 242);
 	       	            MIPsynopsis synDS2T4 = new MIPsynopsis(colDS2T4, 1000, 242);
 	       	            MIPsynopsis synDS2T5 = new MIPsynopsis(colDS2T5, 1000, 242);
 	             	    MIPsynopsis synDS2T6 = new MIPsynopsis(colDS2T6, 1000, 242);
 	       	            MIPsynopsis synDS2T7 = new MIPsynopsis(colDS2T7, 1000, 242);
 	       	       
 	       	            MIPsynopsis synDS3T1 = new MIPsynopsis(colDS3T1, 1000, 242);
	       	            MIPsynopsis synDS3T2 = new MIPsynopsis(colDS3T2, 1000, 242);
	       	            MIPsynopsis synDS3T3 = new MIPsynopsis(colDS3T3, 1000, 242);
	       	            MIPsynopsis synDS3T4 = new MIPsynopsis(colDS3T4, 1000, 242);
	       	            MIPsynopsis synDS3T5 = new MIPsynopsis(colDS3T5, 1000, 242);
 	       	           
	       	            Synopsis synUnionDS1T1_DS2T1 = synDS1T1.union(synDS2T1);
 	       	           
	       	            Synopsis synDS1 = synDS1T1;
 	       	            Synopsis synDS2 = synDS2T1;
 	       	            Synopsis synDS3 = synDS3T1;         	           
 	       	            
 	       	            synDS1=synDS1.union(synDS1T2);
 	         	        synDS1=synDS1.union(synDS1T3);
 	       	            synDS1=synDS1.union(synDS1T4);
 	       	            synDS1=synDS1.union(synDS1T5);
 	                    synDS1=synDS1.union(synDS1T6);
 	                    synDS1=synDS1.union(synDS1T7);
 	      
 	                    synDS2=synDS2.union(synDS2T2);
	         	        synDS2=synDS2.union(synDS2T3);
	       	            synDS2=synDS2.union(synDS2T4);
	       	            synDS2=synDS2.union(synDS2T5);
	                    synDS2=synDS2.union(synDS2T6);
	                    synDS2=synDS2.union(synDS2T7);
	                    
	                    synDS3=synDS3.union(synDS3T2);
	         	        synDS3=synDS3.union(synDS3T3);
	       	            synDS3=synDS3.union(synDS3T4);
	       	            synDS3=synDS3.union(synDS3T5);
	                   
	                  
	                    Synopsis synUnionDS1_DS2 = synDS1.union(synDS2);
	                    Synopsis synIntersectDS1_DS2 = synDS1.intersect(synDS2);
	                    
	                    System.out.println("size of synDS1: " + synDS1.getOriginalSize());
	                    System.out.println("size of synDS2: " + synDS2.getOriginalSize());
 	       	          // System.out.println("Resemblance of synDS1T1,synDS2T1: " + synDS1T1.resemblance(synDS2T1));
 	       	           //System.out.println("intersection size of synDS1T1,synDS2T1 = " + synDS1T7.intersect(synDS2T7).getOriginalSize());
 	       	           //System.out.println("Union-size of synDS1T1, synDS2T1 is = " + synUnionDS1T1_DS2T1.getOriginalSize());
 	       	        
 	       	           System.out.println("Resemblance of synDS1,synDS2: " + synDS1.resemblance(synDS2));
	       	           System.out.println("intersection size of synDS1,synDS2 = " + synDS1.intersect(synDS2).getOriginalSize());
	       	           System.out.println("Union-size of synDS1, synDS2 is = " + synDS1.union(synDS2).getOriginalSize());
	       	           
	       	           System.out.println("Resemblance of synDS1,synDS3: " + synDS1.resemblance(synDS3));
	       	           System.out.println("intersection size of synDS1,synDS3 = " + synDS1.intersect(synDS3).getOriginalSize());
	       	           System.out.println("Union-size of synDS1, synDS3 is = " + synDS1.union(synDS3).getOriginalSize());
	       	           
	       	        System.out.println("Union-size of synDS1,DS2, synDS3 is = " + synUnionDS1_DS2.union(synDS3).getOriginalSize());
    	    /*    for(int i=0; i< strArr1.length;i++)
    	        {
                  set3[i]=  strArr1[i].hashCode();    
                  //System.out.print(strArr1[i]);
                  //System.out.println(" = "+ set3[i]);
    	        }
    	        System.out.println("----------------------");
    	        for(int i=0; i< strArr2.length;i++)
    	        {
                  set4[i]=  strArr2[i].hashCode();    
                  //System.out.print(strArr2[i]); 
                  //System.out.println(" = "+ set4[i]);
    	        }
    	        
    	        System.out.println("----------------------");
    	       
    	        for(int i=0; i< strArr3.length;i++)
    	        {
                  set5[i]=  strArr3[i].hashCode();    
                  //System.out.print(strArr2[i]); 
                  //System.out.println(" = "+ set4[i]);
    	        }
    	        long[] set1 = new long[]{1,2,3,4,50,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
    	        long[] set2 = new long[]{1,20,3,4,5,6,7,8,9,10,111,142,143,114,145,126,1237,138,119,202};
    	        
    	        Collection col1 = new ArrayCollection(set1);
    	        Collection col2 = new ArrayCollection(set2);
    	        Collection col3 = new ArrayCollection(set3);
    	        Collection col4 = new ArrayCollection(set4);
    	        Collection col5 = new ArrayCollection(set5);
    	        
    	        MIPsynopsis syn1 = new MIPsynopsis(col1, 1000, 242);
    	        MIPsynopsis syn2 = new MIPsynopsis(col2, 1000, 242);
    	        MIPsynopsis syn3 = new MIPsynopsis(col3, 1000, 242);
    	        MIPsynopsis syn4 = new MIPsynopsis(col4, 1000, 242);
    	        MIPsynopsis syn5 = new MIPsynopsis(col5, 1000, 242);
    	       
    	        Synopsis syn6 = syn1.union(syn2);
    	        Synopsis syn7 = syn3.union(syn4);
    	    
    	        //System.out.println("Union-size of syn1, syn2 is = " + syn3.getOriginalSize());
    	       // System.out.println("Resemblance of syn1,syn2: " + syn1.resemblance(syn2)+ " and syn3,syn2:" + syn5.resemblance(syn2));
    	      // System.out.println("intersection of syn1,syn2: "+ syn1.intersectionSize(syn2) );
    	     //  System.out.println( syn1.intersectionSize(syn2));
    	   
    	       
     	       System.out.println("intersection size of syn3,syn4 = " + (syn3.intersectionSize(syn4)-1));
     	       System.out.println("Resemblance of syn3,syn4: " + syn3.resemblance(syn4));
     	       System.out.println("Resemblance of syn7,syn4 = " + syn7.resemblance(syn4));*/
    	        
    	        
    	        
    	        //    System.out.println("syn1.noveltyWithSynopsis(syn2)"+(syn1.noveltyWithSynopsis(syn2)));
    	       // String y = "saleemdfdfdsdfdfsdsdfsdasdfsdfsdf";
    	       // System.out.println(y.hashCode());
    	       // BigInteger bi = new BigInteger(y, 36);
    	       // System.out.println(bi);
    	    }
    	    //catch (Exception e) {
    	     
    	        
    	   // }
    	    
    	    
    	//}

		@Override
		public long getEstimatedSize() {
			// TODO Not implemented
			return 0;
		}

		@Override
		public int synopsisSizeInBytes() {
			// TODO Not implemented
			return 0;
		}


}
