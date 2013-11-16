package mapred;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mapred.JobTracker.NOTIFY_RESULT;

import dfs.DataNodeI;

import Common.Pair;
import Common.Util;

public class TaskTracker extends UnicastRemoteObject implements TaskTrackerI{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6393332207622467820L;

	private static String confPath = "conf/mapred.conf";
	
	private static String dfsPath = "conf/dfs.conf";
	
	private static String slavePath = "conf/slaves";
	
//	/* max number of mappers run on one machine */
//	public static Integer maxMappers;
//	
//	/* max number of reducers run on one machine */
//	public static Integer maxReducers;
	
	public static Integer maxTasks;
	
	/* job tracker's host address */
	public static String jobHostname;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* data node's port number */
	private static Integer dataNodePort;
	
	/* data node's service name */
	private static String dataNodeServiceName;
	
	/* name node's host name */
	private static String nameNodeHostname;
	
	/* name node's service name */
	private static String nameNodeServiceName;
	
	/* data chunk size */
	private static Integer chunksize;
	
	/* registry port number */
	private static Integer nameRegPort;
	private static Integer dataRegPort;
	public static Integer jobRegPort;
	public static Integer taskRegPort;
	/* file path (store map and reduce class */
	private static String sysFilePath;
	
	/* intermediate file path (store intermediate result of mappers) */
	private static String interFilePath;
	
	/* minimun chunks */
	private static Integer minChunk;
	
	/* job tracker service name */
	public static String jobServiceName;
	
	/* task service name*/
	public static String taskServiceName;
	
	/* number of paritions */
	public static Integer numOfPartitions;
	
	/* host ip address */
	public static String hostAddress;
	
	/* thread executor */
	private static ExecutorService executor;
	
	/* jobid to filename and corresponding chunks */
//	private static ConcurrentHashMap<Integer, HashMap<String, HashSet<String>>> jobToInput
//		= new ConcurrentHashMap<Integer, HashMap<String, HashSet<String>>>();
	
//	/* file chunk to replicas */
//	private static ConcurrentHashMap<String, HashSet<String>> replicas
//		= new ConcurrentHashMap<String, HashSet<String>>();
	
	/* jobid to intermediate file */
	public static ConcurrentHashMap<Integer, HashSet<String[]>> jobToInter
		= new ConcurrentHashMap<Integer, HashSet<String[]>>();
	
	public static ConcurrentHashMap<Integer, Integer> jobToIncompleteMapper
		= new ConcurrentHashMap<Integer, Integer>();
	
//	/* jobid to mapper */
//	private static ConcurrentHashMap<Integer, String> jobToMapper
//		= new ConcurrentHashMap<Integer, String>();
//	
//	/* jobid to reducer */
//	private static ConcurrentHashMap<Integer, String> jobToReducer
//		= new ConcurrentHashMap<Integer, String>();
	
	/* number of mappers running on this task tracker */
	public static Integer numMappers = 0;
	
	/* number of reducers running on this task tracker */
	public static Integer numReducers = 0;

	
	
	public static void increase_ts(Integer t, int n) {
		synchronized(t) {
			t += n;
		}
	}
	public static void decrease_ts(Integer t, int n) {
		synchronized(t) {
			t -= n;
		}
	}
	
	protected TaskTracker() throws RemoteException {
		super();
	}

	@SuppressWarnings("unused")
	@Override
	public void pushMapTask(int jid, JobConf conf, 
			Hashtable<Integer, String> chunks) throws RemoteException {
		
		System.out.println("chunk number:"+chunks.size());
		
		/* distributed chunks */
		int cnt = 0;
		int num = 0;
		Hashtable<Integer, ArrayList<Integer>> pcks = new Hashtable<Integer, ArrayList<Integer>>();
		Hashtable<Integer, ArrayList<String>> dnodes = new Hashtable<Integer, ArrayList<String>>();
		ArrayList<Integer> cks = null;
		ArrayList<String> nodes = null;
		for (int c : chunks.keySet()) {
			if(cnt == minChunk) {
				pcks.put(num, cks);
				dnodes.put(num, nodes);
				cnt = 0;
				num++;
			}
			if (cnt == 0) {
				cks = new ArrayList<Integer>();
				nodes = new ArrayList<String>();
			}
			cks.add(c);
			nodes.add(chunks.get(c));
			cnt++;
		}
		if(cnt != minChunk) {
			pcks.put(num, cks);
			dnodes.put(num, nodes);
			num++;
		}
		
		System.out.println("Mapper:"+num);
		
		Integer n = jobToIncompleteMapper.get(jid);
		
		
		if (n == null)
			n = num;
		else {
			System.out.println(jid+" already run on this machine! previously " + n);
			n += num;
		}
		
		jobToIncompleteMapper.put(jid, n);
		
		System.out.println(jid+" after" + jobToIncompleteMapper.get(jid));

		
		increase_ts(numMappers, num);
		
		JobTrackerI jobtracker = null;
		
		/* read mapper class */
		Registry reg = LocateRegistry.getRegistry(jobHostname, jobRegPort);
		try {
			 jobtracker = (JobTrackerI)reg.lookup(jobServiceName);
			
			Pair mapper = jobtracker.readMapper(jid);
						
			String path = ((String)mapper.name).replace('.', '/');
				
			Util.writeBinaryToFile((byte[])mapper.content, path + ".class");
			
			/* new mapper instance and do job */
			for (int nn = 0; nn < num; nn++) {
				taskMapThread tt = new taskMapThread(jid, pcks.get(nn).size(), conf,
						pcks.get(nn), dnodes.get(nn), dataRegPort,
						dataNodeServiceName, (String)mapper.name, interFilePath, numOfPartitions);
				
				executor.execute(tt);
				
			}
		} catch (NotBoundException e) {
			/* if something bad happend in this function */
			e.printStackTrace();
			if (jobtracker != null) {
				System.out.println("Job "+jid+"failed!");
				jobtracker.notifyMapResult(JobTracker.NOTIFY_RESULT.FAIL, jid, hostAddress);
			}
		}
		
	}


	@Override
	public void pushReduceTask(int jid, JobConf conf, 
			HashSet<String> interNodes, int partition) throws RemoteException {		
		
		JobTrackerI jobtracker = null;
		try {
			
			/* read reducer class */
			
			Registry reg = LocateRegistry.getRegistry(jobHostname, jobRegPort);
			
			jobtracker = (JobTrackerI)reg.lookup(jobServiceName);
			
			Pair reducer = jobtracker.readReducer(jid);
						
			String path = ((String)reducer.name).replace('.', '/');
				
			Util.writeBinaryToFile((byte[])reducer.content, path + ".class");
			
			/* new reducer instance and do job */
			
			taskReduceThread tt = new taskReduceThread(jid, conf, partition, (String) reducer.name, interNodes);
			
			executor.execute(tt);
			
//			runReduceTask(jid, conf, partition, (String)reducer.name, interNodes);
			
		} catch(Exception e) {
			System.out.println("Job "+jid+"failed!");
			jobtracker.notifyReduceResult(JobTracker.NOTIFY_RESULT.FAIL, jid, hostAddress, partition);		
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		try
	    {
			 TaskTracker tasktracker = new TaskTracker();
			 Util.readConfigurationFile(confPath, tasktracker);
			 Util.readConfigurationFile(dfsPath, tasktracker);
			 			 
			 executor = Executors.newFixedThreadPool(maxTasks);
			 
			 if(!sysFilePath.endsWith("/")) {
				 sysFilePath += "/";
			 }
			 
			 if(!interFilePath.endsWith("/")) {
				 interFilePath += "/";
			 }
			 
			 unexportObject(tasktracker, false);
			 TaskTrackerI stub = (TaskTrackerI) exportObject(tasktracker, taskPort);
			 
			 Registry registry = LocateRegistry.createRegistry(taskRegPort);
			 
			 InetAddress address = InetAddress.getLocalHost();
			 
			 System.out.println(address.getHostAddress());
			 
			 hostAddress = address.getHostAddress();
			 
			 registry.rebind(taskServiceName, stub);
			 
			 System.out.println ("TaskTracker ready!");
	    }
	    catch (Exception e)
	    {
	    	
	    	e.printStackTrace();
	    	
	    	System.out.println("Exception happend when running the TaskTracker!");
	    }
	}
	@Override
	public boolean heartBeat() throws RemoteException {
		return true;
	}
	@Override
	public String getInterFiles(int jobid, int partition) throws RemoteException {
		
		HashSet<String[]> filePath = jobToInter.get(jobid);
		
		
		if (filePath == null)
			return null;
		
		System.out.println("file num: "+filePath.size());

		
		StringBuffer res = new StringBuffer("");
		
		int nSize = filePath.size();
		
		if(nSize == 0) {
			return "";
		}
		
		String currentLine[] = new String[nSize];
		String values[] = new String[nSize];
		SortedSet<String> orders = new TreeSet<String>();
		
		InputStreamReader isr[] = new InputStreamReader[nSize];
		BufferedReader br[] = new BufferedReader[nSize];
		FileInputStream in[] = new FileInputStream[nSize];
		int i = 0;
		for(String[] path : filePath) {
			try {
				in[i] = new FileInputStream(interFilePath + path[partition]);
			} catch (FileNotFoundException e) {
				return "";
			}
			isr[i] = new InputStreamReader(in[i]);
			br[i] = new BufferedReader(isr[i]);
			
			String temp;
			try {
				temp = br[i].readLine();
			} catch (IOException e) {
				return "";
			}
			if (temp==null)
				continue;
			String lines[] = temp.split(" ");
			currentLine[i] = lines[0];
			values[i] = lines[1];
			orders.add(currentLine[i]);
			i++;
		}
		
		if(orders.size()==0)
			return "";
		
		while(true) {
			if(orders.size()==0)
				break;
			String minStr = orders.first();
			
			for (i = 0;i<nSize;i++){
				while (currentLine[i] != null && currentLine[i].equals(minStr)) {
					res.append(currentLine[i] + " " + values[i] + "\n");
					String temp = null;
					try {
						temp = br[i].readLine();
					} catch (IOException e) {
						return "";
					}
					if (temp != null) {
						String lines[] = temp.split(" ");
						currentLine[i] = lines[0];
						values[i] = lines[1];
					} else {
						currentLine[i] = null;
					}
				}
			}
			orders.remove(minStr);
			for (i = 0;i<nSize;i++){
				if(currentLine[i] == null)
					continue;
				orders.add(currentLine[i]);
			}
		}
		
		
//		for(String[] path : filePath) {
//			try {
////				System.out.println("filepath: " + interFilePath +path[partition]);
//				String temp = new String(Util.readFromFile(interFilePath + path[partition]), "UTF-8");
//				res.append(temp);
//			} catch (UnsupportedEncodingException e) {
//				e.printStackTrace();
//				return null;
//			}
//		}
		
		//System.out.println("getinterfiles: "+res);
		
		return res.toString();
		
	}
	
	public static void runReduceTask (int jid, JobConf conf, 
			int partition, String classname, Set<String> interNodes){
		
		
		String cmd = "java mapred.ReduceRunner " + classname + " " + jid + " " + conf.getOutputfile() + " " +
				conf.getOutputFormat().getName() + " " + dataRegPort + " " + dataNodeServiceName + " " + 
				taskRegPort+ " " + taskServiceName + " " + nameRegPort + " " +
				nameNodeServiceName + " " + nameNodeHostname + " " + partition + " " + chunksize + " " + interNodes.size() +
				" " + interFilePath;
		
		for (String node : interNodes) {
			cmd += " " + node;
		}
		
		System.out.println(cmd);
		
		JobTrackerI jobtracker = null;
			
		try {
			int exitStatus = Util.buildProcess(cmd);
			System.out.println("exitcode: "+exitStatus);
			Registry reg = LocateRegistry.getRegistry(TaskTracker.jobHostname, TaskTracker.jobRegPort);
			jobtracker = (JobTrackerI)reg.lookup(TaskTracker.jobServiceName);
			
			switch(exitStatus) {
				case 0:
					/* normal exit (notify the job tracker) */
					System.out.println("reducer "+jid+" partition " + partition + " succeed!");
					jobtracker.notifyReduceResult(JobTracker.NOTIFY_RESULT.SUCCESS, jid, hostAddress, partition);
					break;
				case 1:
					/* job fail due to other reasons(datanode write failure or application error) */
					System.out.println("reducer "+jid+" partition " + partition + " failed!");
					jobtracker.notifyReduceResult(JobTracker.NOTIFY_RESULT.FAIL, jid, hostAddress, partition);
					break;
				case 2:
					/* job fail due to nodes having intermediate file fail(need to restart mapper) */
					System.out.println("reducer "+jid+" partition " + partition + " failed due to task tracker failure!");
					jobtracker.notifyReduceResult(JobTracker.NOTIFY_RESULT.TASKNODE_FAIL, jid, hostAddress, partition);
					break;
				default:
					System.out.println("reducer "+jid+" partition " + partition + " failed!");
					jobtracker.notifyReduceResult(JobTracker.NOTIFY_RESULT.FAIL, jid, hostAddress, partition);
					break;	
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		

		
	}
	
}
