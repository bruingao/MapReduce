package mapred;

import java.io.Serializable;

public class JobConf implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 870609109352931265L;
	
	private Class<?> mapperClass;
	private Class<?> reducerClass;
	
	private String inputfile;
	private String outputfile;
	
	public Class<?> getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(Class<?> mapperClass) {
		this.mapperClass = mapperClass;
	}
	public Class<?> getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(Class<?> reducerClass) {
		this.reducerClass = reducerClass;
	}
	public String getInputfile() {
		return inputfile;
	}
	public void setInputfile(String inputfile) {
		this.inputfile = inputfile;
	}
	public String getOutputfile() {
		return outputfile;
	}
	public void setOutputfile(String outputfile) {
		this.outputfile = outputfile;
	}
}
