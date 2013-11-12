package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import dfs.NameNodeI;

import Common.Pair;
import Common.Util;
import Common.jobScheduler;

public class JobTracker extends UnicastRemoteObject implements JobTrackerI {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5442874947046318711L;
	
	private static String confPath = "src/conf/mapred.conf";
	
	private static String slavePath = "src/conf/slaves";
	
	/* max number of mappers run on one machine */
	public static Integer maxMappers;
	
	/* max number of reducers run on one machine */
	public static Integer maxReducers;
	
	/* name node's host address */
	private static String nameNodeHostname;
	
	/* name node's port number */
	private static Integer nameNodePort;
	
	/* name node's service name */
	private static String nameNodeServiceName;
	
	/* job tracker's host address */
	private static String jobHostname;
	
	/* taks tracker's service name */
	private static String taskServiceName;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* registry port number */
	public static Integer registryPort;
	
	/* file path (store map and reduce class */
	private static String sysFilePath;
	
	/* intermediate file path (store intermediate result of mappers) */
	private static String interFilePath;
	
	/* local bonus */
	public static Double localBonus;
	
	/* task bonus */
	public static Double taskBonus;
	
	/* minimum chunks per mapper */
	public static Integer minChunk;
	
	/* global job id increasing */
	private static volatile Integer gjobid;
	
	/* job tracker registry */
	private static Registry registry;
	
	/* dfs Registry */
	private static Registry dfsRegistry;
	
//	/* the job's input file name */
//	private static ConcurrentHashMap<Integer, String> jobToInput
//		= new ConcurrentHashMap<Integer, String>();
//	
//	/* the job's output file path */
//	private static ConcurrentHashMap<Integer, String> jobToOutput
//		= new ConcurrentHashMap<Integer, String>();
	
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
	
		
	private void increaseId() {
		synchronized(gjobid) {
			gjobid++;
		}
	}
	
	protected JobTracker() throws RemoteException {
		super();
		gjobid = 0;
	}
	
	public enum JOB_RESULT {FAIL, SUCCESS, INPROGRESS};
	
	@Override
	public String submitJob(JobConf conf, Pair mapper, Pair reducer) throws RemoteException {
		/* check output file name, if exists return OUTPUTEXISTS */
		/* if input file not found return INPUTNOTFOUND */
		
		dfsRegistry = LocateRegistry.getRegistry(nameNodeHostname, nameNodePort);
		
		NameNodeI namenode = null;
		
		try {
			namenode = (NameNodeI)dfsRegistry.lookup(nameNodeHostname+"/"+nameNodeServiceName);
			if(namenode.checkname(conf.getOutputfile()))
				return "OUTPUTEXISTS";
			if(!namenode.checkname(conf.getInputfile()))
				return "INPUTNOTFOUND";
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			return "FAIL";	
		}
		
		/* write the class to corresponding file (in order for task nodes to read */
		Integer jid = gjobid;
		increaseId();
		
		String mapperPath = sysFilePath + "mapper"+ gjobid;
		String reducerPath = sysFilePath + "reducer"+ gjobid;
		
		Util.writeBinaryToFile((byte[])mapper.content, mapperPath);
		Util.writeBinaryToFile((byte[])reducer.content, reducerPath);
		
//		Util.writeObject(mapperPath, conf.getMapperClass());
//		Util.writeObject(reducerPath, conf.getReducerClass());
		
		
		/* store the path of the calss file and the input and output file path */		
		mappers.put(jid, mapperPath);
		reducers.put(jid, reducerPath);
		
		mappername.put(jid, (String)mapper.name);
		reducername.put(jid, (String)reducer.name);
		
		
		/* call open method of dfs namenode and get the corresponding metadata */
		HashMap<Integer, HashSet<String>> filechunks = namenode.open(conf.getInputfile());
		
		/* according to the metadata, choose proper number of tasktrackers to do the job 
		 * store relative informations in jobScheduler */
		HashMap<String, HashMap<Integer, String>> mapperToChunks = jobScheduler.decideMappers(filechunks,jid);
		
		/* for every mapper transfer the filename, corresponding nodes and corresponding chunk number to it */
		for (String node : mapperToChunks.keySet()) {
			Registry reg = LocateRegistry.getRegistry(node, taskPort);
			try {
				TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(taskServiceName);
				tasktracker.pushMapTask(jid, conf, mapperToChunks.get(node));
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		jobStatus.put(jid, true);
		
		return jid.toString();
	}

	@Override
	public JOB_RESULT checkStatus(Integer jobid) throws RemoteException {
		/* check the status if fail return fail */
		if (!jobStatus.get(jobid))
			return JOB_RESULT.FAIL;
		
		/* check the unimplemented number of mappers and reducers 
		 * if both 0 return success, otherwise return inprogress */
		if (jobScheduler.checkStatus(jobid))
			return JOB_RESULT.SUCCESS;
		else 
			return JOB_RESULT.INPROGRESS;
	}

	@Override
	public double checkMapper(Integer jobid) throws RemoteException {
		/* check the unimplemented number of mappers and report progress */
	
		return jobScheduler.getMapperPercent(jobid);
	}

	@Override
	public double checkReducer(Integer jobid) throws RemoteException {
		/* check the unimplementd number of reducers and report progress */
		
		return jobScheduler.getReducerPercet(jobid);
	}

	@Override
	public Pair readMapper(Integer jid) throws RemoteException {
		Pair pair = new Pair(mappername.get(jid), Util.readFromFile(mappers.get(jid)));

		return pair;
	}

	@Override
	public Pair readReducer(Integer jid) throws RemoteException {
		Pair pair = new Pair(reducername.get(jid), Util.readFromFile(reducers.get(jid)));

		return pair;
	}

	@Override
	public void notifyMapResult(boolean res, int jid, String tnode) throws RemoteException {
		if (res) {
			jobScheduler.mapperSucceed(jid, tnode);
			
			if (checkMapper(jid) <= 0.0) {
				mappers.remove(jid);
				mappername.remove(jid);
				/* start reducers */
				
			}
		} else {
			/* if the job fails */
			Pair newNode = jobScheduler.mapperFail(jid, tnode);
			String opNode = (String) newNode.name;
			HashMap<Integer, String> failChunk = (HashMap<Integer, String>) newNode.content;
			
			Registry reg = LocateRegistry.getRegistry(opNode, taskPort);
			try {
				TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(taskServiceName);
				tasktracker.pushMapTask(jid, confs.get(jid), failChunk);
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
