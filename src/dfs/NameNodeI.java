package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;


public interface NameNodeI extends Remote{
	public HashMap<Integer, HashSet<String>> open(String filename) throws RemoteException;
	public HashMap<Integer, HashSet<String>> writeFile(String filename, int num) throws RemoteException;
	public void writeSucess(String filename, boolean res) throws RemoteException;
	public ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> listFiles() throws RemoteException;
	public ConcurrentHashMap<String, HashSet<String>> listNodes() throws RemoteException;
	public void removeFile(String filename) throws RemoteException;
	//public void proxyRebind(String dataNodeServiceName, DataNodeI datanode) throws RemoteException;
}
