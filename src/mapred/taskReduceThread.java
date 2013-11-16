package mapred;

import java.util.Set;

/**
 * taskReduceThread is thread running on the TaskTracker,
 * it runs a reducer task on the TaskTracker.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class taskReduceThread implements Runnable {

    private int jid;
    
    private JobConf conf;
    
    private int partition;
    
    private String classname;
    
    private Set<String> interNodes;

    public taskReduceThread(int jid, JobConf conf, int partition,
            String classname, Set<String> interNodes) {
        super();
        this.jid = jid;
        this.conf = conf;
        this.partition = partition;
        this.classname = classname;
        this.interNodes = interNodes;
    }
    
    /** 
     * thread run function, run a reduce task on the TaskTracker
     * 
     * @since           1.0
     */
    @Override
    public void run() {
        TaskTracker.runReduceTask(jid, conf, partition, classname, interNodes);
    }

}
