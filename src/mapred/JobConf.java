package mapred;

import java.io.Serializable;

public class JobConf implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 870609109352931265L;
	
	private static Class<?> mapperClass;
	private static Class<?> reducerClass;
	
	private static String inputfile;
	private static String outputfile;
	
	public static Class<?> getMapperClass() {
		return mapperClass;
	}
	public static void setMapperClass(Class<?> mapperClass) {
		JobConf.mapperClass = mapperClass;
	}
	public static Class<?> getReducerClass() {
		return reducerClass;
	}
	public static void setReducerClass(Class<?> reducerClass) {
		JobConf.reducerClass = reducerClass;
	}
	public static String getInputfile() {
		return inputfile;
	}
	public static void setInputfile(String inputfile) {
		JobConf.inputfile = inputfile;
	}
	public static String getOutputfile() {
		return outputfile;
	}
	public static void setOutputfile(String outputfile) {
		JobConf.outputfile = outputfile;
	}
}
