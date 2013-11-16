package Common;

import java.util.*;

public final class Partitioner {
    
    public Partitioner()
    {

    }
    
    public static StringBuffer[] partition(SortedMap<Object, ArrayList<Object>> collection, Integer partitionNum){
        
    	System.out.println("begin partition");
    	
    	HashMap<Object, Object> keyHashcode=new HashMap<Object,Object>();
        StringBuffer[] partitions=new StringBuffer[partitionNum];
        
        System.out.println("uniqueKeys: "+collection.size());
        
        for (int i=0;i<partitionNum;i++)
        	partitions[i] = new StringBuffer("");
        
        for (Object key: collection.keySet()){
            keyHashcode.put(key,(key.hashCode())%partitionNum);
        }
        
        for (Object key : collection.keySet()){
        	int index=(int) keyHashcode.get(key);
        	for(Object value : collection.get(key)) {
	            partitions[index].append(key);
	            partitions[index].append(" ");
	            partitions[index].append(value);
	            partitions[index].append("\n");
        	}
            
        }
        
//        System.out.println(partitions[1].toString());
        
//        for(int i=0;i<partitionNum;i++){
//        	System.out.println(partitions[i]);
//        }
        
        return partitions;
    }

}
