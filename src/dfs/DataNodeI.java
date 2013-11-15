package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;


/**
 * DataNodeI is the interface for DataNodes. It defines interfaces to
 * read from local file chunk, write to a local file chunk, remove local 
 * file chunk, respond to heartbeat status check, create a replica of
 * file chunk.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public interface DataNodeI extends Remote{

    /* read from local file chunk and return the content as byte array */
    public byte[] read(String filename) throws RemoteException;
    
    /* write the byte array as content to a local file chunk */
    public void write(String filename, byte[] content) throws RemoteException;
    
    /* remove local file chunk */
    public void removeFile(String filename) throws RemoteException;
    
    /* respond true to heartbeat status check */
    public boolean heartBeat() throws RemoteException;
    
    /* make a replica of the file chunk from one of the residing DataNode */
    public boolean replication(String filename, String[] nodes) throws RemoteException;
}
