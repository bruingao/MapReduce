package mapred;

import java.util.Set;

public class taskReduceThread implements Runnable {

	private int jid;
	
	private JobConf conf;
	
	private int partition;
	
	private String classname;
	
	private Set<String> interNodes;

	public taskReduceThread(int jid, JobConf conf, int partition,
			String classname, Set<String> interNodes) {
		super();
		this.jid = jid;
		this.conf = conf;
		this.partition = partition;
		this.classname = classname;
		this.interNodes = interNodes;
	}
	
	@Override
	public void run() {
		TaskTracker.runReduceTask(jid, conf, partition, classname, interNodes);
	}

}
