package mapred;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class TaskTracker extends UnicastRemoteObject implements TaskTrackerI{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6393332207622467820L;

	private static String confPath = "src/conf/mapred.conf";
	
	private static String slavePath = "src/conf/slaves";
	
	/* max number of mappers run on one machine */
	private static Integer maxMappers;
	
	/* max number of reducers run on one machine */
	private static Integer maxReducers;
	
	/* job tracker's host address */
	private static String jobHostname;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* registry port number */
	private static Integer registryPort;
	
	/* file path (store map and reduce class */
	private static String sysFilePath;
	
	/* intermediate file path (store intermediate result of mappers) */
	private static String interFilePath;
	
	/* jobid to filename and corresponding chunks */
	private static ConcurrentHashMap<Integer, HashSet<String>> jobToInput
		= new ConcurrentHashMap<Integer, HashSet<String>>();
	
	/* file chunk to replicas */
	private static ConcurrentHashMap<String, HashSet<String>> replicas
		= new ConcurrentHashMap<String, HashSet<String>>();
	
	/* jobid to intermediate file */
	private static ConcurrentHashMap<Integer, HashSet<String>> jobToInter
		= new ConcurrentHashMap<Integer, HashSet<String>>();
	
	protected TaskTracker() throws RemoteException {
		super();
	}


	@Override
	public void pushMapTask(int Jobid, String mapperPath,
			HashMap<String, HashSet<String>> files) throws RemoteException {
		/* store the files information in jobs and replicas */
		
		/* read files */
		
		/* read mapper class */
		
		/* new mapper instance and do job */
		
		/* store the intermediate result */
		
		/* notify the job tracker */
		
	}


	@Override
	public void pushReduceTask(int jobid, String reducerPath,
			HashMap<String, String> files) throws RemoteException {
		/* store the intermediate file information */
		
		/* read the intermediate files*/
		
		/* read reducer class */
		
		/* store the final result */
		
		/* notify the job tracker */
	}
	
	

}
