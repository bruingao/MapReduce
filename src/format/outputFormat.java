package format;

import java.util.*;

import Common.Pair;


/**
 * outputFormat is the output format from user programs in
 * MapReduce framework. Each line will be key string, space,
 * value string.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class outputFormat extends inputFormatAbs{

    /** 
     * constructor of outputFormat
     * 
     * @param content   list of kv pairs
     * @since           1.0
     */
    public outputFormat(List<Pair> kvPairs) {
        this.kvPairs=kvPairs;
    }
    
    /** 
     * get list of kv pairs
     * 
     * @return          list of kv pairs
     * @since           1.0
     */
    public List<Pair> getkvPairs(){
        return kvPairs;
    }
    
    /** 
     * convert list kv pairs to output string
     * 
     * @return          string buffer of output
     * @since           1.0
     */
    public StringBuffer getOutput() {
    	StringBuffer sb=new StringBuffer("");
        for (int i=0;i<kvPairs.size();i++){
        	sb.append(kvPairs.get(i).name);
        	sb.append(" ");
        	sb.append(kvPairs.get(i).content);
        	sb.append("\n");
        }
        return sb;
    }
    
}
