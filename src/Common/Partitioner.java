package Common;

import java.util.*;

import format.inputFormatAbs;

public final class Partitioner {

    //HashMap<String, Integer> keyHashcode=new HashMap<String,Integer>();
    //String[] partitions;
    
    public Partitioner()
    {

    }
    
    public static String[] partition(List<inputFormatAbs.kvPair> collection, HashSet<String> uniqueKeys, Integer partitionNum){
        HashMap<String, Integer> keyHashcode=new HashMap<String,Integer>();
        String[] partitions=new String[partitionNum];
        
        for (String key:uniqueKeys){
            keyHashcode.put(key,(key.hashCode())%partitionNum);
        }
        
        for (inputFormatAbs.kvPair pair : collection){
            int index=keyHashcode.get(pair.key);
            if(partitions[index]==null){
                partitions[index] = pair.key + " ";
            }else{
                partitions[index] = partitions[index] + pair.key + " ";
            }
            partitions[index] = partitions[index] + pair.value + "\n";
        }
        
        return partitions;
    }

}
