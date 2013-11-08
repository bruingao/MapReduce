package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;

public interface NameNodeI extends Remote{
	public HashMap<Integer, HashSet<String>> open(String filename) throws RemoteException;
	public HashMap<Integer, HashSet<String>> writeFile(String filename, int lines) throws RemoteException;
	public void writeSucess(String filename, boolean res) throws RemoteException;
}
