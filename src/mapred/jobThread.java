package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

public class jobThread implements Runnable{
	
	private String node;
	private int jid;
	private JobConf conf;
	
	/* what map task need. file chunks and corresponding nodes */
	private Hashtable<Integer, String> chunks;
	
	/* reduce task needs this. 
	 * Means that where the intermediate file reside on */
	private HashSet<String> interNodes;
	
	/* if this is a map task set this flag to true
	 * if this is a reduce task set this flag to false */
	private boolean mapOrReduce;
	
	/* which partition*/
	private int whichPartition;
		
	public jobThread(String node, int jid, JobConf conf,
			Hashtable<Integer, String> chunks, HashSet<String> interNodes, boolean flag, int wp) {
		super();
		this.node = node;
		this.jid = jid;
		this.conf = conf;
		this.chunks = chunks;
		this.mapOrReduce = flag;
		this.interNodes = interNodes;
		this.whichPartition = wp;
	}

	@Override
	public void run() {
		try {
			Registry reg = LocateRegistry.getRegistry(node, JobTracker.taskRegPort);
			TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(JobTracker.taskServiceName);
			if(mapOrReduce)
				tasktracker.pushMapTask(jid, conf, chunks);
			else
				tasktracker.pushReduceTask(jid, conf, interNodes, whichPartition);


		} catch (NotBoundException | RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
