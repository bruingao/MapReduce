package mapred;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;

public interface TaskTrackerI extends Remote{
	public void pushMapTask(int jobid, String mapperPath, HashMap<String,HashSet<String>> files) throws RemoteException;
	public void pushReduceTask(int jobid, String reducerPath, HashMap<String, String> files) throws RemoteException;

}
