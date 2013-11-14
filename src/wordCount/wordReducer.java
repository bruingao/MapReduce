package wordCount;

import java.util.ArrayList;

import Common.Collector;
import mapred.Reducer;

public class wordReducer implements Reducer{

	@Override
	public void reduce(String key, ArrayList<String> value, Collector collector) {
		int sum = 0;
		int i = 0;
		int size = value.size();
	    while (i < size) {
	      int v = Integer.parseInt(value.get(i));
	      sum += v; 
	      i++;
	    }

	    collector.collect(key, sum);
	}

}
