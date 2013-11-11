//package format;

import java.util.*;

public class inputFormatLine extends inputFormatAbs{

    public inputFormatLine(String content) {
        this.content=content;
    }
    
  
    public List<kvPair> getkvPairs() {
        String[] lines=content.split("\n");
        for (int i=0;i<lines.length;i++){
            String[] words=lines[i].trim().split(" ");
            for (int j=0;j<words.length;j++){
                kvPairs.add(new kvPair(Integer.toString(i),words[j].trim()));
            }
        }
        
        return kvPairs;
    }
    
}
