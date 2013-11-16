package format;

import java.util.*;

import Common.Collector;
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
public class outputFormat{

    /**
     * convert list kv pairs to output string
     *
     * @return          string buffer of output
     * @since           1.0
     */
    public StringBuffer getOutput(Collector collector) {
        StringBuffer sb=new StringBuffer("");
        for (Object key : collector.collection.keySet())
        {
            for (Object value : collector.collection.get(key)){
                sb.append(key);
                sb.append(" ");
                sb.append(value);
                sb.append("\n");
            }
        }
        return sb;
    }
    
}
