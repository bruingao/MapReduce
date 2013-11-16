package format;

import java.util.*;

import Common.Pair;


/**
 * inputFormatLine is one recognized input format of
 * MapReduce programs written by user. Key will be
 * line number; value will be splitted string.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */

public class inputFormatLine extends inputFormatAbs{

    /**
     * constructor of inputFormatLine
     *
     * @param content   line content string
     * @since           1.0
     */
    public inputFormatLine(String content) {
        this.content=content;
    }
    
    /**
     * split line string and get values
     *
     * @return          list of kv pairs
     * @since           1.0
     */
    public List<Pair> getkvPairs() {
        String[] lines=content.split("\n");
        for (int i=0;i<lines.length;i++){
            String[] words=lines[i].trim().split(" ");
            for (int j=0;j<words.length;j++){
                if(!words[j].equals(""))
                    kvPairs.add(new Pair(Integer.toString(i),words[j].trim()));
            }
        }
        
        return kvPairs;
    }
    
}
