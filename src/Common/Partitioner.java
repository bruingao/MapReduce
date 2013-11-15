package Common;

import java.util.*;

public final class Partitioner {
    
    public Partitioner()
    {

    }
    
    public static StringBuffer[] partition(List<Pair> collection, HashSet<Object> uniqueKeys, Integer partitionNum){
        
    	System.out.println("begin partition");
    	
    	HashMap<Object, Object> keyHashcode=new HashMap<Object,Object>();
        StringBuffer[] partitions=new StringBuffer[partitionNum];
        
        System.out.println("collection: "+collection.size());
        System.out.println("uniqueKeys: "+uniqueKeys.size());
        
        for (int i=0;i<partitionNum;i++)
        	partitions[i] = new StringBuffer("");
        
        for (Object key:uniqueKeys){
            keyHashcode.put(key,(key.hashCode())%partitionNum);
        }
        
        for (Pair pair : collection){
            int index=(int) keyHashcode.get(pair.name);
            
            partitions[index].append(pair.name);
            partitions[index].append(" ");
            partitions[index].append(pair.content);
            partitions[index].append("\n");
            
        }
        
//        for(int i=0;i<partitionNum;i++){
//        	System.out.println(partitions[i]);
//        }
        
        return partitions;
    }

}
