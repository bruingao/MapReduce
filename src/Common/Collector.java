package Common;

import java.util.*;

public class Collector {

    
    public List<Pair> collection=new ArrayList<Pair>();
    
    public HashSet<Object> uniqueKeys= new HashSet<Object>();
    
    public Collector()
    {
        
    }
    
    public void addkvPairs(List<Pair> kvPairs){
    	collection.addAll(kvPairs);
        for (int i=0;i<kvPairs.size();i++){
            uniqueKeys.add(kvPairs.get(i).name);
        }
    }
    
    public void addkvPair(Pair kvPair){
        collection.add(kvPair);
        uniqueKeys.add(kvPair.name);
    }
    
    public void collect(Object key, Object value) {
    	Pair pair = new Pair(key, value);
    	addkvPair(pair);
    }
    
    public void sortStringKey(){
        Collections.sort(this.collection, new stringKeyComparator());
    }
    
    public void sortIntKey(){
        Collections.sort(this.collection, new intKeyComparator());
    }
    
    public class stringKeyComparator implements Comparator<Pair> {
        public int compare(Pair kvPair1, Pair kvPair2) {
            if (((String) kvPair1.name).hashCode() > ((String) kvPair2.name).hashCode()){
                return 1;
            } else {
                return -1;
            }
        }
    }
    
    public class intKeyComparator implements Comparator<Pair> {
        public int compare(Pair kvPair1, Pair kvPair2) {
            if (Integer.parseInt((String)kvPair1.name)>Integer.parseInt((String)kvPair2.name)){
                return 1;
            } else {
                return -1;
            }
        }
    }
    
    

}
