package mapred;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;

public interface TaskTrackerI extends Remote{
	public void pushMapTask(int jobid, HashMap<Integer, HashSet<String>> replicas,
			String filename, HashSet<Integer> chunks) throws RemoteException;
	public void pushReduceTask(int jobid, HashMap<String, String> files) throws RemoteException;

}
