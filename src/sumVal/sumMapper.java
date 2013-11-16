package wordCount;

import Common.Collector;
import mapred.Mapper;

public class sumMapper implements Mapper{

    @Override
    public void map(Object key, Object value, Collector collector) {
        String newKey = (String)key;
        int newValue = Integer.parseInt((String)value);
        collector.collect(newKey, newValue);
    }
    
}
