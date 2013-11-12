package mapred;

import format.Collector;

public interface Mapper {
	public void map(String key, String value, Collector collector);
}
