package Common;

import java.io.Serializable;

public class Pair implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -766889303484035685L;
	public String name;
	public byte[] content;
	
	public Pair(String n, byte[] c) {
		name = n;
		content = c;
	}
}
