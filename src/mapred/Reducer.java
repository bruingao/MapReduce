package mapred;

import java.util.ArrayList;

import Common.Collector;


/**
 * Reducer is the interface for users to write their own reducer class
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public interface Reducer {
    public void reduce(String key, ArrayList<String> value, Collector collector);
}
