package Common;

import java.io.Serializable;

/**
 * Pair is class defining the generalized form of key value pair
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class Pair implements Serializable{

    private static final long serialVersionUID = -766889303484035685L;
    public Object name;
    public Object content;
    
    /** 
     * constructor of DataNode class
     * 
     * @param n         the key (name) object
     * @param c         the value (content) object
     * @since           1.0
     */    
    public Pair(Object n, Object c) {
        name = n;
        content = c;
    }
    
    /** 
     * determine if two pairs are equal
     * 
     * @param e         destination pair to be compared with self
     * @return          true if equal; false otherwise
     * @since           1.0
     */
    @Override
    public boolean equals(Object e) {
        return name.equals(((Pair)e).name) && content.equals(((Pair)e).content);
    }
}
