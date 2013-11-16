package format;

import java.util.*;

import Common.Pair;

public class inputFormatPair extends inputFormatAbs{

    public inputFormatPair(String content) {
        this.content=content;
    }
    
  
    public List<Pair> getkvPairs() {
        String[] lines=content.split("\n");
        for (int i=0;i<lines.length;i++){
            String[] words=lines[i].trim().split(" ");
            kvPairs.add(new Pair(words[0].trim(),words[1].trim()));
        }
        
        return kvPairs;
    }
    
}
