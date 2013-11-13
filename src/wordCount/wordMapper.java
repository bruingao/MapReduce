package wordCount;

import Common.Collector;
import mapred.Mapper;

public class wordMapper implements Mapper{

	@Override
	public void map(Object key, Object value, Collector collector) {
		String newKey = (String)value;
		int newValue = 1;
		collector.collect(newKey, newValue);
	}
	
}
