package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;


public interface NameNodeI extends Remote{
	public Hashtable<Integer, HashSet<String>> open(String filename) throws RemoteException;
	public Hashtable<Integer, HashSet<String>> writeFile(String filename, int num) throws RemoteException;
	public void writeSucess(String filename, boolean res) throws RemoteException;
	public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> listFiles() throws RemoteException;
	public ConcurrentHashMap<String, HashSet<String>> listNodes() throws RemoteException;
	public void removeFile(String filename) throws RemoteException;
	public boolean checkname (String filename) throws RemoteException;
	//public void proxyRebind(String dataNodeServiceName, DataNodeI datanode) throws RemoteException;
}
