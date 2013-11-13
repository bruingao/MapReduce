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
        HashMap<String, Object> keyHashcode=new HashMap<String,Object>();
        String[] partitions=new String[partitionNum];
        
        for (Object key:uniqueKeys){
            keyHashcode.put((String)key,(key.hashCode())%partitionNum);
        }
        
        for (Pair pair : collection){
            int index=(int) keyHashcode.get(pair.name);
            if(partitions[index]==null){
                partitions[index] = pair.name + " ";
            }else{
                partitions[index] = partitions[index] + pair.name + " ";
            }
            partitions[index] = partitions[index] + pair.content + "\n";
        }
        
        return partitions;
    }

}
