package mapred;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dfs.NameNodeI;

import Common.Pair;
import Common.Util;
import Common.jobScheduler;


/**
 * JobTracker is the only one controlling hub (or master node) of
 * the MapReduce framework. It manages many aspects of the framework,
 * including accepting jobs from client programs, recording the
 * detailed informations about jobs (like mapper/reducer classes and
 * other configurations) locally on the JobTracker,  dispatching
 * corresponding mapper/reducer tasks to the TaskTrackers where the
 * parallel computing happens (via jobThread which will push a task
 * to a specified TaskTracker), keeping monotonically increasing
 * jobID, communicating with NameNode in distributed file system for
 * informations about file chunk locations, heart beating and checking
 * status of each TaskTracker and when failure happens restarting the
 * necessary tasks on another chosen TaskTracker, checking task
 * progress and responding to the queries from client programs,
 * handling mapper/reducer notifications in order to trigger next
 * steps, repeat necessary steps or report failure to the client
 * (under extreme circumstances), terminating jobs, etc.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class JobTracker extends UnicastRemoteObject implements JobTrackerI {

    private static final long serialVersionUID = 5442874947046318711L;
    
    private static final String confPath = "conf/mapred.conf";
    
    private static final String slavePath = "conf/slaves";
    
    private static final String dfsPath = "conf/dfs.conf";
    
    private static final String idPath = "conf/jobid"; 
    
    /* possible max number of tasks run on one machine */
    public static Integer maxTasks;
    
//    /* max number of reducers run on one machine */
//    public static Integer maxReducers;
    
    /* name node's host address */
    private static String nameNodeHostname;
    
    /* name node's port number */
//    private static Integer nameNodePort;
    
    /* name node's service name */
    private static String nameNodeServiceName;
    
    /* job tracker's host address */
//    private static String jobHostname;
    
    /* job trackers' service name */
    private static String jobServiceName;
    
    /* taks tracker's service name */
    public static String taskServiceName;
    
    /* job tracker's port number */
    private static Integer jobPort;
    
    /* task tracker's port number */
//    private static Integer taskPort;
    
    /* maximum fail times */
    private static Integer maxFailTask;
    
    /* number of partitions */
    public static Integer numOfPartitions;
    
    /* registry port number */
    private static Integer nameRegPort;
//    private static Integer dataRegPort;
    private static Integer jobRegPort;
    public static Integer taskRegPort;
    
    /* file path (store map and reduce class */
    private static String sysFilePath;
    
    /* intermediate file path (store intermediate result of mappers) */
//    private static String interFilePath;
    
    /* local bonus */
    public static Double localBonus;
    
    /* task bonus */
    public static Double taskBonus;
    
    /* minimum chunks per mapper */
    public static Integer minChunk;
    
    /* global job id increasing */
    private static volatile Integer gjobid;
            
    private static ExecutorService executor = Executors.newCachedThreadPool();
    
    /* the job's mapper class path */
    private static ConcurrentHashMap<Integer, String> mappers
        = new ConcurrentHashMap<Integer, String>();
    
    /* the job's reducer class path */
    private static ConcurrentHashMap<Integer, String> reducers
        = new ConcurrentHashMap<Integer, String>();
    
    /* the job's mapper class name */
    private static ConcurrentHashMap<Integer, String> mappername
    = new ConcurrentHashMap<Integer, String>();
    
    /* the job's reducer class name */
    private static ConcurrentHashMap<Integer, String> reducername
        = new ConcurrentHashMap<Integer, String>();
    
    /* job to its conf */
    private static ConcurrentHashMap<Integer, JobConf> confs
        = new ConcurrentHashMap<Integer, JobConf>();
    
    /* the job's current status fail or normal */
    private static ConcurrentHashMap<Integer, Boolean> jobStatus
        = new ConcurrentHashMap<Integer, Boolean>();
    
    private static ConcurrentHashMap<Integer, Boolean> jobMapperFinished
        = new ConcurrentHashMap<Integer, Boolean>();
    
    private static ConcurrentHashMap<Integer, Integer> jobToFailTimes
        = new ConcurrentHashMap<Integer, Integer>();
    
    /** 
     * check status of TaskTrackers and reschedule if failure is found
     * 
     * @since           1.0
     */
    private void checkNode() {
        for (String node : jobScheduler.nodeStatus.keySet()){
            try {
                Registry reg = LocateRegistry.getRegistry(node, taskRegPort);
                TaskTrackerI tasktracker = (TaskTrackerI)reg.lookup(taskServiceName);
                boolean status = tasktracker.heartBeat();
                jobScheduler.nodeStatus.put(node, status);
                
            } catch (Exception e) {
                System.out.println("node failure!");
                jobScheduler.nodeStatus.put(node, false);
                HashSet<Integer> mapjobs = new HashSet<Integer>(jobScheduler.nodeToMapJobs.get(node));

                for (int jid : mapjobs) {
                    
                    System.out.println("recovering node failure!");

                    
                    /* choose a new task tracker and extract the failed chunks */
                    Pair newNode = jobScheduler.mapperFail(jid, node, null);
                    /* new task tracker */
                    
                    System.out.println("newNode" + newNode.name);
                    
                    if(newNode == null) {
                        jobStatus.put(jid, false);
                        continue;
                    }
                    
                    String opNode = (String) newNode.name;
                    
                    
                    /* failed chunks and corresponding datanode */
                    Hashtable<Integer, String> failChunk = (Hashtable<Integer, String>) newNode.content;
                    
                    jobThread t = new jobThread(opNode, jid, confs.get(jid), failChunk, null, true, -1);
                    
                    executor.execute(t);
                }
                
                System.out.println("recovery mapper succeed!");

                
                HashSet<Pair> reduceJobs = new HashSet<Pair>(jobScheduler.nodeToReduceJobs.get(node));
                
                for(Pair pair : reduceJobs) {
                    /* reducer fail */
                    
                    int jid = (int)pair.name;
                    int partition = (int)pair.content;
                    HashSet<String> mapperTrackers = new HashSet<String>(jobScheduler.jobToMappers.get(jid).keySet());

                    String newNode = jobScheduler.reducerFail(jid, node, partition);
                    
                    if(newNode == null) {
                        jobStatus.put(jid, false);
                        continue;
                    }
                    
                    /* push reducer task */
                    jobThread t = new jobThread(newNode, jid, confs.get(jid), 
                            null, mapperTrackers, false, partition);
                    
                    executor.execute(t);
                }
                
            }
        }
    }
    
    /** 
     * timer for checking status of TaskTrackers
     * 
     * @since           1.0
     */
    public void checkTimer() {
        Timer check = new Timer();
        check.scheduleAtFixedRate (new TimerTask(){
            
            @Override
            public void run() {
                checkNode();
            }
        },0, 5000);
    }
    
    /** 
     * increase the monotonically increasing jod id
     * 
     * @since           1.0
     */
    private void increaseId() {
        synchronized(gjobid) {
            gjobid++;
            Util.writeObject(idPath, gjobid);
        }
    }
    
    /** 
     * constructor of JobTracker
     * 
     * @since           1.0
     */
    protected JobTracker() throws RemoteException {
        super();
        gjobid = 0;
    }
    
    public enum JOB_RESULT {FAIL, SUCCESS, INPROGRESS};
    
    /** 
     * accept jobs submitted by users, open jobThreads to
     * push the tasks to TaskTrackers
     * 
     * @param conf      job configurations
     * @param mapper    mapper class info
     * @param reducer   reducer class info
     * @return          status string
     * @since           1.0
     */ 
    @Override
    public String submitJob(JobConf conf, Pair mapper, Pair reducer) throws RemoteException {
        /* check output file name, if exists return OUTPUTEXISTS */
        /* if input file not found return INPUTNOTFOUND */
        
        Registry dfsRegistry = LocateRegistry.getRegistry(nameNodeHostname, nameRegPort);
        
        NameNodeI namenode = null;
        
        try {
            namenode = (NameNodeI)dfsRegistry.lookup(nameNodeServiceName);
            
            if(!namenode.checkname(conf.getInputfile()))
                return "INPUTNOTFOUND";
        } catch (NotBoundException e) {
            return "FAIL";    
        }
        
        /* write the class to corresponding file (in order for task nodes to read */
        Integer jid = gjobid;
        increaseId();

        /* call open method of dfs namenode and get the corresponding metadata */
        Hashtable<Integer, HashSet<String>> filechunks = namenode.open(conf.getInputfile());
        
        /* according to the metadata, choose proper number of tasktrackers to do the job 
         * store relative informations in jobScheduler */
        Hashtable<String, Hashtable<Integer, String>> mapperToChunks = jobScheduler.decideMappers(filechunks,jid);
        
        /* if there are problems choosing the mapper task trackers */
        if (mapperToChunks == null)
            return "FAIL";
        
        
        String mapperPath = sysFilePath + "mapper"+ gjobid;
        String reducerPath = sysFilePath + "reducer"+ gjobid;
        
        Util.writeBinaryToFile((byte[])mapper.content, mapperPath);
        Util.writeBinaryToFile((byte[])reducer.content, reducerPath);

        /* store the path of the calss file and the input and output file path */        
        mappers.put(jid, mapperPath);
        reducers.put(jid, reducerPath);
                
        
        mappername.put(jid, (String)mapper.name);
        reducername.put(jid, (String)reducer.name);
        
        confs.put(jid, conf);
        
        System.out.println("submit" +jid+" "+mappers.get(jid)+" "+mappername.get(jid));
        
        /* for every mapper transfer the filename, corresponding nodes and corresponding chunk number to it */
        for (String node : mapperToChunks.keySet()) {
            System.out.println("choose node: " +node);
            
            jobThread t = new jobThread(node, jid, conf, mapperToChunks.get(node), null, true, -1);
            
            executor.execute(t);
            
        }
        
        jobStatus.put(jid, true);
        jobMapperFinished.put(jid, false);
        jobToFailTimes.put(jid, 0);
        
        return jid.toString();
    }

    /** 
     * check the status of an accepted job
     * 
     * @param jid       the job id
     * @return          job result
     * @since           1.0
     */ 
    @Override
    public JOB_RESULT checkStatus(Integer jobid) throws RemoteException {
        /* check the status if fail return fail */
        if ( jobStatus.get(jobid) == null || !jobStatus.get(jobid))
            return JOB_RESULT.FAIL;
        
        /* check the unimplemented number of mappers and reducers 
         * if both 0 return success, otherwise return inprogress */
        if (jobScheduler.checkStatus(jobid))
            return JOB_RESULT.SUCCESS;
        else 
            return JOB_RESULT.INPROGRESS;
    }

    /** 
     * check the progress of mappers of an accepted job
     * 
     * @param jid       the job id
     * @return          progress (0-1)
     * @since           1.0
     */  
    @Override
    public double checkMapper(Integer jobid) throws RemoteException {
        /* check the unimplemented number of mappers and report progress */
    
        return jobScheduler.getMapperPercent(jobid);
    }

    /** 
     * check the progress of reducers of an accepted job
     * 
     * @param jid       the job id
     * @return          progress (0-1)
     * @since           1.0
     */ 
    @Override
    public double checkReducer(Integer jobid) throws RemoteException {
        /* check the unimplementd number of reducers and report progress */
        
        return jobScheduler.getReducerPercet(jobid);
    }

    /** 
     * read mapper class info of a job
     * 
     * @param jid       the job id
     * @return          pair of mapper class info
     * @since           1.0
     */ 
    @Override
    public Pair readMapper(Integer jid) throws RemoteException {
        System.out.println("read"+jid+" "+mappers.get(jid)+" "+mappername.get(jid));
        
        System.out.println(mappers.get(jid));
        Pair pair = new Pair(mappername.get(jid), Util.readFromFile(mappers.get(jid)));

        return pair;
    }
    
    /** 
     * read reducer class info of a job
     * 
     * @param jid       the job id
     * @return          pair of reducer class info
     * @since           1.0
     */ 
    @Override
    public Pair readReducer(Integer jid) throws RemoteException {
        Pair pair = new Pair(reducername.get(jid), Util.readFromFile(reducers.get(jid)));

        return pair;
    }
    
    public enum NOTIFY_RESULT {FAIL, SUCCESS, DATANODE_FAIL, TASKNODE_FAIL};
    
    /** 
     * accept mapper result notifications
     * 
     * @param res       the result
     * @param jid       job id
     * @param tnode     node hostname
     * @since           1.0
     */ 
    @Override
    public void notifyMapResult(NOTIFY_RESULT res, int jid, String tnode) throws RemoteException {
        if(jobStatus.get(jid) == null)
            return;
        
        if (res == NOTIFY_RESULT.SUCCESS) {
            
            jobScheduler.mapperSucceed(jid, tnode);
            System.out.println("map "+jid+" on node "+tnode +" succeed!");

            synchronized(jobMapperFinished.get(jid)){
                if (jobMapperFinished.get(jid))
                    return;
                if (checkMapper(jid) >= 1.0) {
                    /* should not remove mappers's information 
                     * in case of jobtracker failure in the process 
                     * of retrieving intermediate data by reducers */
    //                mappers.remove(jid);
    //                mappername.remove(jid);
                    /* start reducers */
                    ArrayList<String> dreducers = jobScheduler.decideReducers(jid, numOfPartitions);
                    
                    /* if there are problems when choosing the reduce tasktrackers */
                    if (dreducers == null) {
                        jobStatus.put(jid, false);
                        return;
                    }
                    /* push reduce tasks to decided task trackers */
                    else {
                        HashSet<String> mapperTrackers = new HashSet<String>( jobScheduler.jobToMappers.get(jid).keySet());
                        /* for every partition choose a corresponding reducer to do the work */
                        for (int i = 0; i <numOfPartitions; i++) {
                            /* get all the map task trackers of this job */
                            System.out.println("choose reduce task tracker:" + dreducers.get(i));
                            
                            /* push reducer task */
                            jobThread t = new jobThread(dreducers.get(i), jid, confs.get(jid), 
                                    null, mapperTrackers, false, i);
                            
                            executor.execute(t);
                            
                        }
                    }
                    jobMapperFinished.put(jid, true);
                }
            }
        } else {
            /* if the job fails */
            if(jobToFailTimes.get(jid) + 1 >= maxFailTask) {
                jobStatus.put(jid, false);
                return;
            }
            
            jobToFailTimes.put(jid, jobToFailTimes.get(jid)+1);
            
            try {
                Hashtable<Integer,HashSet<String>> chunks = null;
                if(res == NOTIFY_RESULT.DATANODE_FAIL) {
                    Registry nameRegistry = LocateRegistry.getRegistry(nameNodeHostname, nameRegPort);
                    /* check the datanodes */
                    NameNodeI namenode = (NameNodeI) nameRegistry.lookup(nameNodeServiceName);
                    chunks = namenode.open(confs.get(jid).getInputfile());
                }
                
                /* choose a new task tracker and extract the failed chunks */
                Pair newNode = jobScheduler.mapperFail(jid, tnode,chunks);
                
                if(newNode == null) {
                    jobStatus.put(jid, false);
                    return;
                }
                
                /* new task tracker */
                String opNode = (String) newNode.name;
                /* failed chunks and corresponding datanode */
                Hashtable<Integer, String> failChunk = (Hashtable<Integer, String>) newNode.content;
                
                jobThread t = new jobThread(opNode, jid, confs.get(jid), failChunk, null, true, -1);
                
                executor.execute(t);
                
//                Registry reg = LocateRegistry.getRegistry(opNode, taskRegPort);
//                
//                TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(taskServiceName);
//                tasktracker.pushMapTask(jid, confs.get(jid), failChunk);
                
            } catch (NotBoundException e) {
                e.printStackTrace();
            }
            
        }
        
    }
    
    /** 
     * read the configuration file of list of slave nodes 
     * 
     * @param filepath  slaves configuration file path
     * @since           1.0
     */ 
    public static void readSlaves(String filepath) throws UnsupportedEncodingException{
        String content = new String(Util.readFromFile(filepath),"UTF-8");
        
        String lines[] = content.split("\n");
        for(int i = 0; i < lines.length; i++) {
            jobScheduler.nodeToNumTasks.put(lines[i], 0);
            jobScheduler.nodeStatus.put(lines[i], false);
            jobScheduler.nodeToMapJobs.put(lines[i],new HashSet<Integer>());
            jobScheduler.nodeToReduceJobs.put(lines[i], new HashSet<Pair>());
        }    
    }
    
    /** 
     * accept reducer result notifications
     * 
     * @param res       the result
     * @param jid       job id
     * @param tnode     node hostname
     * @param partition partition number
     * @since           1.0
     */ 
    @Override
    public void notifyReduceResult(NOTIFY_RESULT res, int jid, String tnode, int partition)
            throws RemoteException {
        if(jobStatus.get(jid) == null)
            return;
        
        if (res == NOTIFY_RESULT.SUCCESS) {
            
            /* reducer succeed */
            System.out.println("reduce "+jid+" partition "+partition+" on node "+tnode +" succeed!");
            jobScheduler.reducerSucceed(jid, tnode, partition);
        } else if (res == NOTIFY_RESULT.FAIL){
            if(jobStatus.get(jid) == null)
                return;
            
            if(jobToFailTimes.get(jid) + 1 >= maxFailTask) {
                jobStatus.put(jid, false);
                return;
            }
            
            jobToFailTimes.put(jid, jobToFailTimes.get(jid)+1);
            
            System.out.println("reduce "+jid+" partition "+partition+" on node "+tnode +" failed!");
            /* reducer fail */
            HashSet<String> mapperTrackers = new HashSet<String>(jobScheduler.jobToMappers.get(jid).keySet());

            String newNode = jobScheduler.reducerFail(jid, tnode, partition);
            
            if(newNode == null) {
                jobStatus.put(jid, false);
                return;
            }
            
            System.out.println("newNode" + newNode);

            
            jobThread t = new jobThread(newNode, jid, confs.get(jid), 
                    null, mapperTrackers, false, partition);
            
            executor.execute(t);
            
//            tasktracker.pushReduceTask(jid, confs.get(jid), mapperTrackers, partition);
            
            System.out.println("reduce "+jid+" partition "+partition+" continues on node "+tnode);
        } else if (res == NOTIFY_RESULT.TASKNODE_FAIL) {
            
            System.out.println("reduce "+jid+" partition "+partition+" on node "+tnode +" failed!");

            jobStatus.put(jid, false);
        }
    }
    
    /** 
     * remove all info about a job from the MapReduce framework
     * 
     * @param jid       job id
     * @since           1.0
     */ 
    private static void removeAll(int jid) {
        if(!jobStatus.containsKey(jid))
            return;
            
        mappers.remove(jid);

        reducers.remove(jid);

        mappername.remove(jid);

        reducername.remove(jid);

        confs.remove(jid);
            
        jobScheduler.removeAll(jid);
        
        jobToFailTimes.remove(jid);
        
        jobStatus.remove(jid);
    }
    
    /** 
     * the main function of JobTracker, read configuration files,
     * register itself as service in local RMI registry, then
     * periodically check the status of TaskTrackers.
     * 
     * @since           1.0
     */ 
    public static void main(String[] args) {
        try
        {
             JobTracker jobtracker = new JobTracker();
             Util.readConfigurationFile(confPath, jobtracker);
             Util.readConfigurationFile(dfsPath, jobtracker);
             readSlaves(slavePath);
             
             if(!sysFilePath.endsWith("/")) {
                 sysFilePath += "/";
             }
             
             Integer temp = (Integer) Util.readObject(idPath);
             if(temp != null)
                 gjobid = temp;
             
             /* check the status of slaves */
             
             unexportObject(jobtracker, false);
             JobTrackerI stub = (JobTrackerI) exportObject(jobtracker, jobPort);
             
             Registry registry = LocateRegistry.createRegistry(jobRegPort);
             
             InetAddress address = InetAddress.getLocalHost();
             
             System.out.println(address.getHostAddress());
             
             registry.rebind(jobServiceName, stub);
             
             System.out.println ("JobTracker ready!");
             
             jobtracker.checkTimer();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Exception happend when running the JobTracker!");
        }
    }

    /** 
     * terminate a job
     * 
     * @param jid       job id
     * @since           1.0
     */ 
    @Override
    public void terminateJob(int jid) throws RemoteException {
        removeAll(jid);
    }

    
}
