package mapred;

import Common.Collector;

public interface Reducer {
	public void reduce(String key, String value, Collector collector);
}
