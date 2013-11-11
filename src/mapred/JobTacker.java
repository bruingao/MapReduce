package mapred;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

public class JobTacker extends UnicastRemoteObject implements JobTrackerI {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5442874947046318711L;
	
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
	
	/* registry */
	Registry registry;
	
	/* the job's input file name */
	private static ConcurrentHashMap<Integer, String> jobToInput
		= new ConcurrentHashMap<Integer, String>();
	
	/* the job's output file path */
	private static ConcurrentHashMap<Integer, String> jobToOutput
		= new ConcurrentHashMap<Integer, String>();
	
	/* the job's mapper class name */
	private static ConcurrentHashMap<Integer, String> mappers
		= new ConcurrentHashMap<Integer, String>();
	
	/* the job's reducer class name */
	private static ConcurrentHashMap<Integer, String> reducers
		= new ConcurrentHashMap<Integer, String>();
	
	protected JobTacker() throws RemoteException {
		super();
	}
	
	public enum JOB_RESULT {OUTPUTEXISTS, INPUTNOTFOUND, FAIL, SUCCESS, INPROGRESS};
	
	@Override
	public JOB_RESULT submitJob(JobConf conf) throws RemoteException {
		/* write the class to corresponding file (in order for task nodes to read */
		
		/* store the path of the calss file and the input and output file path */
		
		/* check output file name, if exists return OUTPUTEXISTS */
		
		/* call open method of dfs namenode and get the corresponding metadata
		 * if file not found return INPUTNOTFOUND */
		
		/* according to the metadata, choose proper number of tasktrackers to do the job 
		 * store relative informations in jobScheduler */
		
		return JOB_RESULT.FAIL;
	}

	@Override
	public JOB_RESULT checkStatus(Integer jobid) throws RemoteException {
		/* check the unimplemented number of mappers and reducers 
		 * if both 0 return success, otherwise return inprogress */
		
		
		return null;
	}

	@Override
	public double checkMapper(Integer jobid) throws RemoteException {
		/* check the unimplemented number of mappers and report progress */
		
		return 0;
	}

	@Override
	public double checkReducer(Integer jobid) throws RemoteException {
		/* check the unimplementd number of reducers and report progress */
		
		return 0;
	}

}
