package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;


/**
 * NameNodeI is the interface for NameNode. It defines interfaces to
 * get the locations of chunks when opening file, get the locations of
 * chunks when writing file to DFS, update the information and checkpoint
 * after a write, list all files in DFS, list all DataNodes in DFS,
 * remove file from DFS, check if a file exist in DFS.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public interface NameNodeI extends Remote{

    /* pass the locations of chunks when opening a file in DFS */
    public Hashtable<Integer, HashSet<String>> open(String filename) throws RemoteException;
    
    /* pass the destination locations of chunks when writing a file to DFS */
    public Hashtable<Integer, HashSet<String>> writeFile(String filename, int num) throws RemoteException;
    
    /* update the information and checkpoint after a write */
    public void writeSucess(String filename, boolean res) throws RemoteException;
    
    /* list all of the files stored on the DFS, along with locations of file chunks */
    public ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> listFiles() throws RemoteException;
    
    /* list all of the DataNodes and the file chunks on the DataNodes */
    public ConcurrentHashMap<String, HashSet<String>> listNodes() throws RemoteException;
    
    /* remove a file from DFS */
    public void removeFile(String filename) throws RemoteException;
    
    /* check if a file exists in DFS */
    public boolean checkname (String filename) throws RemoteException;
}
