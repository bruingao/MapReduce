package mapred;

import Common.Collector;

public interface Mapper {
	public void map(Object key, Object value, Collector collector);
}
