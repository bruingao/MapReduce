package format;

import java.util.*;

import Common.Pair;


/**
 * inputFormatPair is one recognized input format of
 * MapReduce programs written by user. Key will be
 * the first splitted string in line; value will be
 * the second splitted string.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class inputFormatPair extends inputFormatAbs{

    /**
     * constructor of inputFormatPair
     *
     * @param content   line content string
     * @since           1.0
     */
    public inputFormatPair(String content) {
        this.content=content;
    }
    
    /**
     * split line string and get keys and values
     *
     * @return          list of kv pairs
     * @since           1.0
     */
    public List<Pair> getkvPairs() {
        String[] lines=content.split("\n");
        for (int i=0;i<lines.length;i++){
            String[] words=lines[i].trim().split(" ");
            kvPairs.add(new Pair(words[0].trim(),words[1].trim()));
        }
        
        return kvPairs;
    }
    
}
