package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeI extends Remote{
	public byte[] read(String filename) throws RemoteException;
	
	public void write(String filename, byte[] content) throws RemoteException;
	
	public void removeFile(String filename) throws RemoteException;
	
	public boolean heartBeat() throws RemoteException;
	
	public boolean replication(String filename, String[] nodes) throws RemoteException;
}
