//package format;

import java.util.*;

public class outputFormat extends inputFormatAbs{

    public outputFormat(List<kvPair> kvPairs) {
        this.kvPairs=kvPairs;
    }
    
    public List<kvPair> getkvPairs(){
        return kvPairs;
    }
  
    public String getOutput() {
        content=new String();
        for (int i=0;i<kvPairs.size();i++){
            content=content+kvPairs.get(i).key+" ";
            content=content+kvPairs.get(i).value+"\n";
        }
        return content;
    }
    
}
