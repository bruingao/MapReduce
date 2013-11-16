package dfs;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;

import Common.Util;

/**
 * DataNode is the class running on a DataNode. It reads configuration
 * files to get parameters and list of DataNode hostnames, creates local
 * RMI registry and bind itself as a service. The DataNode is able to 
 * recover from failures, read/write/remove local file chunks, make replica
 * of file chunks, and respond to heartbeat status checks.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class DataNode extends UnicastRemoteObject implements DataNodeI{
    
    private static final long serialVersionUID = 2961863470847180775L;
    
    /* configuration file */
    private static String confPath = "conf/dfs.conf";
    
    /* DataNode hostname list file */
    private static String dnPath = "conf/slaves";
    
    /* RMI registry on the DataNode */
    private static  Registry registry;
    
    /* set of local file replicas (filename + chunk number) */
    private static volatile HashSet<String> files
        = new HashSet<String>();
    
    /* set of DataNode hostnames */
    private static volatile HashSet<String> datanodes 
        = new HashSet<String>();
    
    /* paremeters read from the configuration file */
    private static String dataNodePath;
    private static String nameNodeHostname;
    private static Integer nameRegPort;
    private static Integer dataRegPort;
    private static Integer dataNodePort;   
    private static String dataNodeServiceName;
    private static String nameNodeServiceName;
    
    /** 
     * constructor of DataNode class
     * 
     * @since           1.0
     */
    public DataNode() throws RemoteException{
        
    }
    
    /** 
     * thread-safely add a string to a hashset
     * 
     * @param obj       the hashset
     * @param filename  the string to be added to the hashset
     * @since           1.0
     */
    private static void add_ts(HashSet<String> obj, String filename) {
        synchronized(obj) {
            files.add(filename);
        }
    }
    
    /** 
     * thread-safely remove a string from a hashset
     * 
     * @param obj       the hashset
     * @param filename  the string to be removed from the hashset
     * @since           1.0
     */
    private static void remove_ts(HashSet<String> obj, String filename) {
        synchronized(obj) {
            files.remove(filename);
        }
    }
    
    /** 
     * recover the DataNode from local checkpoint
     * 
     * @since           1.0
     */
    private void Init() {
        Object obj = Util.readObject(dataNodePath+"files");
        if(obj != null)
            files = (HashSet<String>) obj;
    }
    
    /** 
     * read the content of a file chunk
     * 
     * @param filename  the file name of the chunk to be read
     * @return          the byte array of the file chunk
     * @since           1.0
     */
    @Override
    public byte[] read(String filename) {
        String temp = dataNodePath + filename;
        return Util.readFromFile(temp);
    }
    
    /** 
     * write the byte array to a file chunk
     * 
     * @param filename  the file name of the chunk
     * @param content   the byte array to be written to file chunk
     * @since           1.0
     */
    @Override
    public void write(String filename, byte[] content) {
        String temp = dataNodePath + filename;
        System.out.println(content);
        System.out.println(temp);
        Util.writeBinaryToFile(content, temp);
        
        /* update local information */
        add_ts(files,filename);
        /* check point */
        Util.checkpointFiles(dataNodePath + "files", files);
    }
    
    /** 
     * remove a local file chunk, and then update local information
     * and  checkpoint
     * 
     * @param filename  the file name of the chunk to be removed
     * @since           1.0
     */
    @Override
    public void removeFile(String filename) throws RemoteException {
    
        /* remove a local file chunk */
        try{
            File file = new File(dataNodePath+filename);
            file.setWritable(true);
            if(file.delete()){
            
            }else{
                System.out.println("Delete operation is failed.");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        
        /* update local information */
        remove_ts(files,filename);
        /* check point */
        Util.checkpointFiles(dataNodePath + "files", files);
    }

    /** 
     * read the list of DataNode hostnames and store them
     * 
     * @param filename  the file name of DataNode list file
     * @since           1.0
     */
    public static void readDataNodes(String filename) throws UnsupportedEncodingException {
        String content = new String(Util.readFromFile(filename),"UTF-8");
        
        String lines[] = content.split("\n");
        for(int i = 0; i < lines.length; i++) {
            add_ts(datanodes,lines[i]);
        }    
    }
    
    /** 
     * respond to status check from the NameNode
     * 
     * @return          true indicating DataNode is alive
     * @since           1.0
     */
    @Override
    public boolean heartBeat() {
        return true;
    }
    
    /** 
     * make a replica of the specified file chunk from one of the residing DataNodes,
     * and write the replica on local DataNode
     * 
     * @param filename  the file name of the chunk to be replicated
     * @param nodes     the residing nodes
     * @return          true if success, false otherwise
     * @since           1.0
     */
    @Override
    public boolean replication(String filename, String[] nodes)
            throws RemoteException {
        if(nodes.length <= 0)
            return false;
        int index = 0;
        
        try {
        
            Registry dnRegistry=LocateRegistry.getRegistry(nodes[index],dataRegPort);
            DataNodeI datanode = (DataNodeI)dnRegistry.lookup(dataNodeServiceName);
            byte[] content = datanode.read(filename);
            this.write(filename, content);
            return true;
        } catch (NotBoundException e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /** 
     * The main function on the DataNode. It reads the configuration file
     * and DataNode list, recovers from local checkpoint, creates a local 
     * RMI registry and binds the DataNode on the registry as a service.
     * 
     * @param args      no arguments needed, parameters can be written in the configuration file
     * @since           1.0
     */
    public static void main(String args[]) {
        
        try
        {
             DataNode datanode = new DataNode();
             Util.readConfigurationFile(confPath, datanode);
             readDataNodes(dnPath);
             datanode.Init();
             
             if(!dataNodePath.endsWith("/")) {
                 dataNodePath += "/";
             }
             
             unexportObject(datanode, false);
             DataNodeI stub = (DataNodeI) exportObject(datanode, dataNodePort);
             registry = LocateRegistry.createRegistry(dataRegPort);
             InetAddress address = InetAddress.getLocalHost();
             System.out.println(address.getHostAddress());
             registry.rebind(dataNodeServiceName, stub);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Exception happend when running the Datanode!");
        }
        
    }

}
