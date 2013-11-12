package mapred;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;

import Common.Util;

public class taskThread implements Runnable{

	private int jid;
	private int numOfChunks;
	private JobConf conf;
	private HashSet<Integer> chunks;
	private HashSet<String> dataNodeHost;
	private int regPort;
	private String serviceName;
	
	private String classname;
	
	public taskThread(int jid, int numOfChunks, JobConf conf,
			HashSet<Integer> chunks, HashSet<String> dataNodeHost, int regPort,
			String serviceName, String classname) {
		super();
		this.jid = jid;
		this.numOfChunks = numOfChunks;
		this.conf = conf;
		this.chunks = chunks;
		this.dataNodeHost = dataNodeHost;
		this.regPort = regPort;
		this.serviceName = serviceName;
		this.classname = classname;
	}
	
	@Override
	public void run() {
		
		String cmd = "java "+ "MapRunner" + " " + this.classname + " " + jid + " " + numOfChunks
				+ " " + conf.getInputfile() + " " + conf.getInputFormat().getName() + " " ;
		
		for (int c : chunks) {
			cmd += c + " ";
		}
		
		cmd += dataNodeHost + " " + regPort + " " + serviceName;
		
		System.out.println(cmd);
		
		ProcessBuilder process = new ProcessBuilder(cmd);
		
		try {
			Process task = process.start();
			InputStream str = task.getInputStream();
			int exitStatus = task.waitFor();
			String response = Util.convertStreamToStr(str);
			
			/* process response and exit status */
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
