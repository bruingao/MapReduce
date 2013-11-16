package dfs;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;

import Common.dfsScheduler;
import Common.Util;

/**
 * checkThread is the class running as a thread of the NameNode.
 * It can periodically check the status of DataNode. Also, it can
 * write a replica of the file chunk on a specified DataNode in
 * order to satisfy replication factor (when some DataNode fails
 * and the number of replicas is smaller than replication factor).
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class checkThread implements Runnable{

    private String dnode;
    //private int dnodeport;
    private int registryPort;
    private String serviceName;
    
    private OP op;

    private String filename;
    
    private int chunknumber;

    private HashSet<String> nodes;

    public enum OP  {WRITE, STATUS, DELETE};
    
    /**
     * constructor of checkThread class
     *
     * @param dn        the hostname of the DataNode to be operated
     * @param rp        the registry port number on the DataNode
     * @param sname     the RMI service name
     * @since           1.0
     */
    public checkThread(String dn, int rp, String sname)
    {
        dnode = dn;
        //dnodeport = dnp;
        registryPort=rp;
        serviceName = sname;
    }
    
    /**
     * set the chunk number of the file chunk to be wrote
     *
     * @param chunknumber   the chunk number of the file chunk
     * @since               1.0
     */
    public void setChunknumber(int chunknumber) {
        this.chunknumber = chunknumber;
    }
    
    /**
     * set the file name of the file chunk to be wrote
     *
     * @param s         the chunk number of the file chunk
     * @since           1.0
     */
    public void setFilename(String s) {
        filename = s;
    }
    
    /**
     * set the operation type of the checkThread
     *
     * @param op        the operation type of the checkThread
     * @since           1.0
     */
    public void setOp(OP op) {
        this.op = op;
    }
    
    /**
     * set the residing nodes of the file chunk to be wrote
     *
     * @param nodes     the set of residing nodes
     * @since           1.0
     */
    public void setNodes(HashSet<String> nodes) {
        this.nodes = nodes;
    }
    
    /**
     * run the thread according to the operation type
     *
     * @since           1.0
     */
    @Override
    public void run() {
        switch(op) {
            case STATUS:
                checkStatus();
                break;
            case WRITE:
                write();
                break;
            case DELETE:
                delete();
                break;
            default:
                break;
        }
    }
    
    /**
     * ckeck the status of the specified DataNode, and then
     * update the status of the DataNode in DFS scheduler.
     *
     * @since           1.0
     */
    private void checkStatus() {
        boolean status = false;
        try {
            Registry dnRegistry=LocateRegistry.getRegistry(dnode,registryPort);
            //DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
            DataNodeI datanode = (DataNodeI) dnRegistry.lookup(serviceName);
            status = datanode.heartBeat();
            
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            if (e instanceof java.rmi.NotBoundException) {
                System.out.println("Datanode "+dnode+" not startup.");
            }
            
            else if(e instanceof java.rmi.ConnectException){
                
            }
            
            else {
                e.printStackTrace();
            }
        } finally {
            dfsScheduler.getStatus().put(dnode, status);
        }
    }
    
    /**
     * write a file chunk replica to the specified DataNode, and then
     * update the information on DFS scheduler and checkpoint on NameNode
     *
     * @since           1.0
     */
    private void write() {
        try {
            Registry dnRegistry=LocateRegistry.getRegistry(dnode,registryPort);
            //DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
            DataNodeI datanode = (DataNodeI) dnRegistry.lookup(serviceName);
            
            String[] temp = new String[nodes.size()];
            
            int cnt = 0;
            
            for(String n : nodes) {
                if(n == dnode)
                    continue;
                
                temp[cnt] = n;
                cnt++;
            }
            
            if(cnt <= 0) {
                return;
            }
            
            boolean res = datanode.replication(filename + "-" + chunknumber, temp);
            
            if (res) {
                dfsScheduler.replication(filename, chunknumber, dnode);
                /* checkpoint */
                Util.checkpointFiles(NameNode.nameNodePath + "files", dfsScheduler.getFiles());
                Util.checkpointFiles(NameNode.nameNodePath+"nodeToReplicas", dfsScheduler.getNodeToReplicas());
            }
            
        } catch (Exception e) {
            if (e instanceof java.rmi.NotBoundException) {
                System.out.println("Datanode "+dnode+" not startup.");
            }
            
            else if(e instanceof java.rmi.ConnectException){
                
            }
            
            else {
                e.printStackTrace();
            }
        } 
    }
    
    /**
     * delete a file chunk replica on the specified DataNode, and then
     * update the information on DFS scheduler and checkpoint on NameNode
     *
     * @since           1.0
     */
    private void delete() {
        try {
            Registry dnRegistry=LocateRegistry.getRegistry(dnode,registryPort);
            DataNodeI datanode = (DataNodeI) dnRegistry.lookup(serviceName);
            datanode.removeFile(filename + "-" + chunknumber);
            
            dfsScheduler.removeReplica(filename, chunknumber, dnode);
            /* checkpoint */
            Util.checkpointFiles(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
            Util.checkpointFiles(NameNode.nameNodePath+"nodeToReplicas", dfsScheduler.getNodeToReplicas());

        } catch (Exception e) {
            if (e instanceof java.rmi.NotBoundException) {
                System.out.println("Datanode "+dnode+" not startup.");
            }
            
            else if(e instanceof java.rmi.ConnectException){
                
            }
            
            else {
                e.printStackTrace();
            }
        } 
    }

}
