package wordCount;

import java.rmi.RemoteException;

import format.inputFormatPair;
import format.outputFormat;
import mapred.JobClient;
import mapred.JobConf;

public class sumVal {
    public static void main(String[] args) {
        JobConf conf = new JobConf();
        conf.setInputfile("inputSum");
        conf.setOutputfile("outputSum.txt");
        conf.setInputFormat(inputFormatPair.class);
        conf.setOutputFormat(outputFormat.class);
        conf.setMapperClass(sumMapper.class);
        conf.setReducerClass(sumReducer.class);
        
        JobClient jobclient = new JobClient();
        
        try {
            jobclient.runJob(conf);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
