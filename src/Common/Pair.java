package Common;

import java.io.Serializable;

public class Pair implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -766889303484035685L;
	public Object name;
	public Object content;
	
	public Pair(Object n, Object c) {
		name = n;
		content = c;
	}
}
