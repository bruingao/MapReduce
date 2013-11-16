package Common;

import java.util.*;

public class Collector {

    
    public SortedMap<Object, ArrayList<Object>> collection=new TreeMap<Object, ArrayList<Object>>();
    
    //public HashSet<Object> uniqueKeys= new HashSet<Object>();
    
    public Collector()
    {
        
    }
    
    public void addkvPairs(SortedMap<Object, ArrayList<Object>> kvPairs){
    	collection.putAll(kvPairs);

    }
    
//    public void addkvPair(Pair kvPair){
//        collection.add(kvPair);
//        uniqueKeys.add(kvPair.name);
//    }
    
    public void collect(Object key, Object value) {
    	
    	if(collection.containsKey(key)) {
    		collection.get(key).add(value);
    	} else {
    		ArrayList<Object> obj = new ArrayList<Object>();
    		obj.add(value);
    		collection.put(key, obj);
    	}
    	
//    	addkvPair(pair);
    }
    
//    public void sortStringKey(){
//        Collections.sort(this.collection, new stringKeyComparator());
//    }
//    
//    public void sortIntKey(){
//        Collections.sort(this.collection, new intKeyComparator());
//    }
//    
//    public class stringKeyComparator implements Comparator<Pair> {
//        public int compare(Pair kvPair1, Pair kvPair2) {
//            if (((String) kvPair1.name).hashCode() > ((String) kvPair2.name).hashCode()){
//                return 1;
//            } else {
//                return -1;
//            }
//        }
//    }
//    
//    public class intKeyComparator implements Comparator<Pair> {
//        public int compare(Pair kvPair1, Pair kvPair2) {
//            if (Integer.parseInt((String)kvPair1.name)>Integer.parseInt((String)kvPair2.name)){
//                return 1;
//            } else {
//                return -1;
//            }
//        }
//    }
//    
    

}
