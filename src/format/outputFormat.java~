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
  
    public StringBuffer getOutput() {
    	StringBuffer sb=new StringBuffer("");
        for (int i=0;i<kvPairs.size();i++){
        	sb.append(kvPairs.get(i).name);
        	sb.append(" ");
        	sb.append(kvPairs.get(i).content);
        	sb.append("\n");
        }
        return sb;
    }
    
}
