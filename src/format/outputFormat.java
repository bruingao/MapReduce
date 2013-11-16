package format;

import java.util.*;

import Common.Collector;
import Common.Pair;

public class outputFormat{
	
//    public outputFormat(List<Pair> kvPairs) {
//        this.kvPairs=kvPairs;
//    }
//    
//    public List<Pair> getkvPairs(){
//        return kvPairs;
//    }
  
    public StringBuffer getOutput(Collector collector) {
    	StringBuffer sb=new StringBuffer("");
    	for (Object key : collector.collection.keySet())
    	{
	        for (Object value : collector.collection.get(key)){
	        	sb.append(key);
	        	sb.append(" ");
	        	sb.append(value);
	        	sb.append("\n");
	        }
    	}
        return sb;
    }
    
}
