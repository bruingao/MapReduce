package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;


/**
 * jobThread is the thread running on JobTracker.
 * It pushes mapper/reducer tasks to specified 
 * TaskTracker.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class jobThread implements Runnable{
    
    private String node;
    private int jid;
    private JobConf conf;
    
    /* what map task need. file chunks and corresponding nodes */
    private Hashtable<Integer, String> chunks;
    
    /* reduce task needs this. 
     * Means that where the intermediate file reside on */
    private HashSet<String> interNodes;
    
    /* if this is a map task set this flag to true
     * if this is a reduce task set this flag to false */
    private boolean mapOrReduce;
    
    /* which partition*/
    private int whichPartition;
    
    /** 
     * constructor of jobThread class
     *
     * @param node          destination node
     * @param jid           job id
     * @param conf          configurations of job
     * @param chunks        task related file chunks
     * @param interNodes    set of intermediate nodes
     * @param flag          true if task is a mapper; false if reducer
     * @param wp            partition number 
     * @since               1.0
     */
    public jobThread(String node, int jid, JobConf conf,
            Hashtable<Integer, String> chunks, HashSet<String> interNodes, boolean flag, int wp) {
        super();
        this.node = node;
        this.jid = jid;
        this.conf = conf;
        this.chunks = chunks;
        this.mapOrReduce = flag;
        this.interNodes = interNodes;
        this.whichPartition = wp;
    }
    
    /** 
     * thread runner, push task to corresponding node
     *
     * @since               1.0
     */
    @Override
    public void run() {
        try {
            Registry reg = LocateRegistry.getRegistry(node, JobTracker.taskRegPort);
            TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(JobTracker.taskServiceName);
            if(mapOrReduce)
                tasktracker.pushMapTask(jid, conf, chunks);
            else
                tasktracker.pushReduceTask(jid, conf, interNodes, whichPartition);


        } catch (NotBoundException | RemoteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    

}
