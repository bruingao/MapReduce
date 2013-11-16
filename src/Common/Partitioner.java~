package Common;

import java.util.*;


/**
 * Partitioner is a utility class partitioning the collected results from mappers
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public final class Partitioner {
    
    /** 
     * constructor of Partitioner class
     * 
     * @since           1.0
     */
    public Partitioner()
    {

    }
    
    /** 
     * partition collection into specified number of partitions
     * 
     * @param collection    kv pair list
     * @param uniqueKeys    hash set of unique keys, facilitating partitioning
     * @param partitionNum  desired partition number
     * @return              string array of partitions
     * @since               1.0
     */
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

        return partitions;
    }

}
