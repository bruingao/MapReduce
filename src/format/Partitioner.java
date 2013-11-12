import java.io.*;
import java.util.*;
import java.lang.Math;

public class Partitioner {

    
    List<inputFormatAbs.kvPair> collection=new ArrayList<inputFormatAbs.kvPair>();
    HashSet<String> uniqueKeys =new HashSet<String>();
    int partitionNum;
    
    HashMap<String, Integer> keyHashcode=new HashMap<String,Integer>();
    String[] partitions;
    
    public Partitioner(List<inputFormatAbs.kvPair> kvPairs, HashSet<String> uKeys, Integer partitionN)
    {
        this.collection=kvPairs;
        this.uniqueKeys=uKeys;
        this.partitionNum=partitionN;
        this.partitions= new String[partitionN];
    }
    
    public String[] partition(){
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
