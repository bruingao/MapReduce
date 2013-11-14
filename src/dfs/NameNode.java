package dfs;

import java.io.UnsupportedEncodingException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Common.dfsScheduler;
import Common.Util;


/**
 * NameNode is the class running on a NameNode. It acts as the controlling 
 * hub which works wit DFS scheduler, dispatches jobs among DataNodes and
 * monitors their status. It reads configuration files to get parameters and
 * list of DataNode hostnames, creates local RMI registry and bind itself as
 * a service. 
 *
 * The NameNode is able to recover from failures, periodically check the 
 * status of DataNodes and the replication of file chunks, dispatch jobs like
 * read/write/remove files. It is capable of listing the information about 
 * DataNodes and files in the DFS.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class NameNode extends UnicastRemoteObject implements NameNodeI{
    
    private static final long serialVersionUID = 7921414827247184085L;

    /* configuration file */
    private static String confPath = "conf/dfs.conf";
    
    /* DataNode hostname list file */
    private static String dnPath = "conf/slaves";
    
    /* paremeters read from the configuration file */
    private static Integer replicaFactor;
    private static String nameNodeServiceName;
    private static String registryHostname;
    private static Integer nameRegPort;
    private static Integer dataRegPort;
    private static Integer nameNodePort;
    private static Integer dataNodePort;
    private static String dataNodeServiceName;
    public static String nameNodePath;
    
    /* RMI registry on the NameNode*/
    public static Registry registry;
    
    private static ExecutorService executor = Executors.newCachedThreadPool();
    
    /** 
     * constructor of NameNode class
     * 
     * @since           1.0
     */
    public NameNode() throws RemoteException{
    
    }
    
    /** 
     * recover the NameNode from local checkpoint
     * 
     * @since           1.0
     */
    private void Init() {
        /* read checkpoint */
        Object obj = Util.readObject(nameNodePath+"files");
        if (obj != null)
            dfsScheduler.setFiles((ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>) obj);
        
        obj = Util.readObject(nameNodePath+"nodeToReplicas");
        if (obj != null)
            dfsScheduler.setNodeToReplicas((ConcurrentHashMap<String, HashSet<String>>) obj);
        
        obj = Util.readObject(nameNodePath+"tempfiles");
        if (obj != null)
            dfsScheduler.setTempFiles((ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>) obj);
    }
    
    
    /** 
     * delete a file in the DFS
     *
     * @since           1.0
     */
    public void deleteFile(){
    
    }
    
    /** 
     * create a checkThread to check the status of DataNedes in the DFS
     * 
     * @since           1.0
     */
    public void checkDataNodes(){
        for (String host : dfsScheduler.getStatus().keySet()) {
            checkThread ct = new checkThread(host, dataRegPort, dataNodeServiceName);
            ct.setOp(checkThread.OP.STATUS);
            executor.execute(ct);
        }
    }
    
    /** 
     * check the replication status of file chunks, if the number 
     * of replicas of a file chunk is smaller than the replication 
     * factor (due to failure of DataNodes), it create a checkThread
     * to write replicas of the file chunk in order to meet the 
     * replication factor.
     * 
     * @since           1.0
     */
    public void checkreplication() {
    
        /* check if each file chunk's number of replicas is equal to replication number */
        for (String name : dfsScheduler.getFiles().keySet()) {
            HashMap<Integer, HashSet<String>> map = dfsScheduler.getFiles().get(name);
            for (Integer i : map.keySet()) {
                int cnt  = 0;
                HashSet<String> candidates = new HashSet<String>();
                for (String datanode : map.get(i)) {
                    if(dfsScheduler.getStatus().get(datanode)) {
                        cnt++;
                        candidates.add(datanode);
                    }
                }
                
                /* 
                 * make more replicas if the number of replica for a file chunk is smaller
                 * than replication factor, get the file from any residing DataNode.
                 */
                if (cnt < replicaFactor) {
                    String res[] = dfsScheduler.chooseLight(replicaFactor - cnt, candidates);
                    if(candidates.size()==0)
						break;						
                    for (String r : res) {
                        if(r == null)
                            break;
                        checkThread t = new checkThread(r, dataRegPort, dataNodeServiceName);
                        t.setFilename(name);
                        t.setChunknumber(i);
                        t.setOp(checkThread.OP.WRITE);
                        t.setNodes(candidates);
                        executor.execute(t);
                    }
                }
            }
        }
    }
    
    /** 
     * pass the file chunk locations when writing a file to DFS, and record them in checkpoint
     * 
     * @param filename  the name of the file to be written
     * @param num       the number of chunks
     * @return          the hashmap of chunk number and hashset of residing DataNodes
     * @since           1.0
     */
    @Override
    public HashMap<Integer, HashSet<String>> writeFile(String filename, int num) throws RemoteException {
        /* get the locations of file chunks from DFS scheduler */
        HashMap<Integer, HashSet<String>> res = dfsScheduler.createFile(filename, num, replicaFactor);
        if (res.size() > 0 && res != null)
            /* check point */
            Util.writeObject(nameNodePath + "tempfiles", dfsScheduler.getTempFiles());
        return res;
    }

    /** 
     * pass the file chunk locations when opening a file in DFS
     * 
     * @param filename  the file name of the file to open
     * @return          the hashmap of chunk number and hashset of residing DataNodes
     * @since           1.0
     */
    @Override
    public HashMap<Integer, HashSet<String>> open(String filename) throws RemoteException {
        /* get the locations of file chunks from DFS scheduler */
        return dfsScheduler.openFile(filename);
    }

    /** 
     * update local information and checkpoint according to if a write succeeded
     * 
     * @param filename  the file name of the file to open
     * @param res       true if write succeeded, false
     * @return          the hashmap of chunk number and hashset of residing DataNodes
     * @since           1.0
     */
    @Override
    public void writeSucess(String filename, boolean res) throws RemoteException {
        if(res) {
            dfsScheduler.transferTemp(filename);
            Util.writeObject(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
            Util.writeObject(NameNode.nameNodePath + "nodeToReplicas", dfsScheduler.getNodeToReplicas());
        }
        else {
            dfsScheduler.deleteTemp(filename);
        }
        
        /* check point */
        Util.writeObject(NameNode.nameNodePath+"tempfiles", dfsScheduler.getTempFiles());
    }
    
    /** 
     * read the hostnames of DataNodes from file and pass them to DFS scheduler
     * 
     * @param filename  the name of the file containing the DataNode hostnames
     * @since           1.0
     */
    public static void readDataNodes(String filename) throws UnsupportedEncodingException {
        String content = new String(Util.readFromFile(filename), "UTF-8");
        String lines[] = content.split("\n");
        for(int i = 0; i < lines.length; i++) {
            dfsScheduler.getStatus().put(lines[i], false);
            dfsScheduler.getNodeToReplicas().put(lines[i], new HashSet<String>());
        }
    }
    
    /** 
     * a timer periodically checking the status of DataNodes and the replication
     * of file chunks; period is 5 seconds since start of NameNode
     * 
     * @since           1.0
     */
    private void checkTimer() {
        Timer check = new Timer();
        check.scheduleAtFixedRate (new TimerTask(){
            @Override
            public void run() {
                checkDataNodes();
                checkreplication();
            }
        },0, 5000);
    }
    
    /** 
     * The main function on the NameNode. It reads the configuration file
     * and DataNode list, recovers from local checkpoint, creates a local 
     * RMI registry and binds the DataNode on the registry as a service,
     * periodically checks the status of DataNodes and the replication of 
     * file chunks.
     * 
     * @param args      no arguments needed, parameters can be written in the configuration file
     * @since           1.0
     */
    public static void main(String []args) {
        
        try
        {
            NameNode server = new NameNode();
            Util.readConfigurationFile(confPath, server);
            readDataNodes(dnPath);
            server.Init();

            unexportObject(server, false);
            NameNodeI stub = (NameNodeI) exportObject(server, nameNodePort);
            registry = LocateRegistry.createRegistry(nameRegPort);
            registry.rebind(nameNodeServiceName, stub);
            System.out.println ("NameNode ready!");
             
            server.checkTimer();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Exception happened when starting the namenode!");
        }
    }

    /** 
     * list all of the files stored on the DFS, along with locations of file chunks
     * 
     * @return          the hashmap of filenames and the hashmap of chunk number and residing DataNodes
     * @since           1.0
     */
    @Override
    public ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> listFiles() throws RemoteException {
        return dfsScheduler.getFiles();
    }

    /** 
     * list all of the DataNodes and the file chunks on the DataNodes
     * 
     * @return          the hashmap of DataNode and residing file chunks
     * @since           1.0
     */
    @Override
    public ConcurrentHashMap<String, HashSet<String>> listNodes() throws RemoteException {
        return dfsScheduler.getNodeToReplicas();
    }
    
    /** 
     * remove a file from DFS
     * 
     * @param filename      the name of the file to be removed
     * @since               1.0
     */
    @Override
    public void removeFile(String filename) throws RemoteException {
        dfsScheduler.removeFile(filename);       
        Util.writeObject(nameNodePath+"files", dfsScheduler.getFiles());
        Util.writeObject(nameNodePath + "nodeToReplicas", dfsScheduler.getNodeToReplicas());
    }

    /** 
     * check if a file is in the DFS
     * 
     * @param filename      the name of the file to find
     * @return              true if the file exists in DFS; false otherwise
     * @since               1.0
     */
    @Override
    public boolean checkname(String filename) throws RemoteException {
        return dfsScheduler.checkname(filename);
    }
        
}
