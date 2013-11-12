package mapred;

import Common.Collector;

public interface Mapper {
	public void map(String key, String value, Collector collector);
}
