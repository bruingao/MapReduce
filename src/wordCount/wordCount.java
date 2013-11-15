package wordCount;

import java.rmi.RemoteException;

import format.inputFormatLine;
import format.outputFormat;
import mapred.JobClient;
import mapred.JobConf;

public class wordCount {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		conf.setInputfile("input");
		conf.setOutputfile("output.txt");
		conf.setInputFormat(inputFormatLine.class);
		conf.setOutputFormat(outputFormat.class);
		conf.setMapperClass(wordMapper.class);
		conf.setReducerClass(wordReducer.class);
		
		JobClient jobclient = new JobClient();
		
		try {
			jobclient.runJob(conf);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
