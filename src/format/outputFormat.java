package format;

import java.util.*;

import Common.Pair;

public class outputFormat extends inputFormatAbs{

    public outputFormat(List<Pair> kvPairs) {
        this.kvPairs=kvPairs;
    }
    
    public List<Pair> getkvPairs(){
        return kvPairs;
    }
  
    public String getOutput() {
        content=new String();
        for (int i=0;i<kvPairs.size();i++){
            content=content+kvPairs.get(i).name+" ";
            content=content+kvPairs.get(i).content+"\n";
        }
        return content;
    }
    
}
