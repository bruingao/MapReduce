package Common;

import java.util.*;

import format.inputFormatAbs;

public final class Partitioner {

    //HashMap<String, Integer> keyHashcode=new HashMap<String,Integer>();
    //String[] partitions;
    
    public Partitioner()
    {

    }
    
    public static String[] partition(List<Pair> collection, HashSet<Object> uniqueKeys, Integer partitionNum){
        HashMap<Object, Object> keyHashcode=new HashMap<Object,Object>();
        String[] partitions=new String[partitionNum];
        
        System.out.println("collection: "+collection.size());
        System.out.println("uniqueKeys: "+uniqueKeys.size());

        
        for (Object key:uniqueKeys){
            keyHashcode.put(key,(key.hashCode())%partitionNum);
        }
        
        for (Pair pair : collection){
            int index=(int) keyHashcode.get(pair.name);
            if(partitions[index]==null){
                partitions[index] = (String)pair.name + " ";
            }else{
                partitions[index] = partitions[index] + pair.name + " ";
            }
            partitions[index] = partitions[index] + pair.content + "\n";
        }
        
        for(int i=0;i<partitionNum;i++){
        	System.out.println(partitions[i]);
        }
        
        return partitions;
    }

}
