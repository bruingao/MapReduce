package format;

import java.util.*;

import Common.Pair;

public class inputFormatLine extends inputFormatAbs{

    public inputFormatLine(String content) {
        this.content=content;
    }
    
  
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
