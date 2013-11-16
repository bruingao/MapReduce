package format;

import java.util.*;

import Common.Pair;


/**
 * inputFormatAbs is an abstract class defining the input format of
 * MapReduce programs written by user.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public abstract class inputFormatAbs{

    
    public List<Pair> kvPairs = new ArrayList<Pair>();

    public String content;
    
    public abstract List<Pair> getkvPairs();
}
