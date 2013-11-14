package mapred;

import java.util.ArrayList;

import Common.Collector;

public interface Reducer {
	public void reduce(String key, ArrayList<String> value, Collector collector);
}
