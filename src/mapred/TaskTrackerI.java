package mapred;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.HashSet;

public interface TaskTrackerI extends Remote{
	public void pushMapTask(int jobid, JobConf conf, Hashtable<Integer, String> chunks) throws RemoteException;
	public void pushReduceTask(int jobid, JobConf conf, HashSet<String> interNodes, int partition) throws RemoteException;
	
	public String getInterFiles(int jobid, int partition) throws RemoteException;
	
	public boolean heartBeat() throws RemoteException;
}
