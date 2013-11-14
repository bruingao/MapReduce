package mapred;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class taskReduceThread implements Runnable{

	/* job id */
	private int jid;
	
	/* job configuration information */
	private JobConf conf;
	
	/* inter file tasknode and service name */
	private int regPort;
	private String serviceName;
	
	/* which partition */
	private int partition;
	
	/* reducer class name */
	private String classname;
	
	/* inter files reside on which nodes*/
	private Set<String> interNodes;
	
	public taskReduceThread(int jid, JobConf conf, int regPort,
			String serviceName, int partition, String classname,
			Set<String> interNodes) {
		super();
		this.jid = jid;
		this.conf = conf;
		this.regPort = regPort;
		this.serviceName = serviceName;
		this.partition = partition;
		this.classname = classname;
		this.interNodes = interNodes;
	}
	
	@Override
	public void run() {
		
		
	}

	

}
