package mapred;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

public interface TaskTrackerI extends Remote{
	public void pushMapTask(int jobid, JobConf conf, HashMap<Integer, String> chunks) throws RemoteException;
	public void pushReduceTask(int jobid, HashMap<String, String> files) throws RemoteException;
	
}
