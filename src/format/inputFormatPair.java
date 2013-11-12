package format;

import java.util.*;

public class inputFormatPair extends inputFormatAbs{

    public inputFormatPair(String content) {
        this.content=content;
    }
    
  
    public List<kvPair> getkvPairs() {
        String[] lines=content.split("\n");
        for (int i=0;i<lines.length;i++){
            String[] words=lines[i].trim().split(" ");
            kvPairs.add(new kvPair(words[0].trim(),words[1].trim()));
        }
        
        return kvPairs;
    }
    
}
