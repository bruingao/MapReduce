package mapred;

import java.io.Serializable;


/**
 * JobConf is the configurator open to application programmers.
 * Programmers can set configurations for their jobs with this
 * class
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class JobConf implements Serializable{
    
    private static final long serialVersionUID = 870609109352931265L;
    
    private Class<?> mapperClass;
    private Class<?> reducerClass;
    
    private String inputfile;
    private String outputfile;
    
    private Class<?> inputFormat;
    private Class<?> outputFormat;
    
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
    public Class<?> getInputFormat() {
        return inputFormat;
    }
    public void setInputFormat(Class<?> inputFormat) {
        this.inputFormat = inputFormat;
    }
    public Class<?> getOutputFormat() {
        return outputFormat;
    }
    public void setOutputFormat(Class<?> outputFormat) {
        this.outputFormat = outputFormat;
    }
}
