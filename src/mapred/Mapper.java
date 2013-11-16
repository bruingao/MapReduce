package mapred;

import Common.Collector;


/**
 * Mapper is the interface for users to write their own mapper class
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public interface Mapper {
    public void map(Object key, Object value, Collector collector);
}
