package Common;

import java.util.*;

import format.inputFormatAbs;

public class Collector {

    
    public List<inputFormatAbs.kvPair> collection=new ArrayList<inputFormatAbs.kvPair>();
    
    public HashSet<String> uniqueKeys= new HashSet<String>();
    
    public Collector()
    {
        
    }
    
    public void addkvPairs(List<inputFormatAbs.kvPair> kvPairs){
    	collection.addAll(kvPairs);
        for (int i=0;i<kvPairs.size();i++){
            uniqueKeys.add(kvPairs.get(i).key);
        }
    }
    
    public void addkvPair(inputFormatAbs.kvPair kvPair){
        collection.add(kvPair);
        uniqueKeys.add(kvPair.key);
    }
    
    public void sortStringKey(){
        Collections.sort(this.collection, new stringKeyComparator());
    }
    
    public void sortIntKey(){
        Collections.sort(this.collection, new intKeyComparator());
    }
    
    public class stringKeyComparator implements Comparator<inputFormatAbs.kvPair> {
        public int compare(inputFormatAbs.kvPair kvPair1, inputFormatAbs.kvPair kvPair2) {
            if (kvPair1.key.compareTo(kvPair2.key)>0){
                return 1;
            } else {
                return -1;
            }
        }
    }
    
    public class intKeyComparator implements Comparator<inputFormatAbs.kvPair> {
        public int compare(inputFormatAbs.kvPair kvPair1, inputFormatAbs.kvPair kvPair2) {
            if (Integer.parseInt(kvPair1.key)>Integer.parseInt(kvPair2.key)){
                return 1;
            } else {
                return -1;
            }
        }
    }

}
