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
        
        return partitions;
    }

}
