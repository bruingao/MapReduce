package mapred;

import java.io.UnsupportedEncodingException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import Common.Pair;
import Common.Util;

import dfs.DataNodeI;

public class TaskTracker extends UnicastRemoteObject implements TaskTrackerI{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6393332207622467820L;

	private static String confPath = "src/conf/mapred.conf";
	
	private static String slavePath = "src/conf/slaves";
	
	/* max number of mappers run on one machine */
	public static Integer maxMappers;
	
	/* max number of reducers run on one machine */
	public static Integer maxReducers;
	
	/* job tracker's host address */
	private static String jobHostname;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* data node's port number */
	private static Integer dataNodePort;
	
	/* data node's service name */
	private static String dataNodeServiceName;
	
	/* registry port number */
	private static Integer registryPort;
	
	/* file path (store map and reduce class */
	private static String sysFilePath;
	
	/* intermediate file path (store intermediate result of mappers) */
	private static String interFilePath;
	
	/* minimun chunks */
	private static Integer minChunk;
	
	/* job tracker service name */
	private static String jobServiceName;
	
	/* jobid to filename and corresponding chunks */
//	private static ConcurrentHashMap<Integer, HashMap<String, HashSet<String>>> jobToInput
//		= new ConcurrentHashMap<Integer, HashMap<String, HashSet<String>>>();
	
//	/* file chunk to replicas */
//	private static ConcurrentHashMap<String, HashSet<String>> replicas
//		= new ConcurrentHashMap<String, HashSet<String>>();
	
	/* jobid to intermediate file */
	private static ConcurrentHashMap<Integer, HashSet<String>> jobToInter
		= new ConcurrentHashMap<Integer, HashSet<String>>();
	
	private static ConcurrentHashMap<Integer, Integer> jobToIncompleteMapper
		= new ConcurrentHashMap<Integer, Integer>();
	
//	/* jobid to mapper */
//	private static ConcurrentHashMap<Integer, String> jobToMapper
//		= new ConcurrentHashMap<Integer, String>();
//	
//	/* jobid to reducer */
//	private static ConcurrentHashMap<Integer, String> jobToReducer
//		= new ConcurrentHashMap<Integer, String>();
	
	/* number of mappers running on this task tracker */
	private static Integer numMappers = 0;
	
	/* number of reducers running on this task tracker */
	private static Integer numReducers = 0;

	
	
	private void increase_ts(Integer t, int n) {
		synchronized(t) {
			t += n;
		}
	}
	private void decrease_ts(Integer t, int n) {
		synchronized(t) {
			t -= n;
		}
	}
	
	protected TaskTracker() throws RemoteException {
		super();
	}

	@Override
	public void pushMapTask(int jid, String filename, 
			HashMap<Integer, String> chunks) throws RemoteException {
		/* store the files information in jobs and replicas */
				
//		/* queue */
//		if (numMappers >= maxMappers) {
//			HashMap<String, HashSet<String>> fileToReplicas = new HashMap<String, HashSet<String>>();
//			for(int i : chunks) {
//				fileToReplicas.put(filename+i, filereplicas.get(i));
//			}
//			jobToInput.put(jid, fileToReplicas);
//			return;
//		}
		
		/* distributed chunks */
		int cnt = 0;
		int num = 0;
		HashSet<HashSet<Integer>> pcks = new HashSet<HashSet<Integer>>();
		HashSet<Integer> cks = null;
		for (int c : chunks.keySet()) {
			if(cnt == minChunk) {
				pcks.add(cks);
				cnt = 0;
				num++;
			}
			if (cnt == 0)
				cks = new HashSet<Integer>();
			cks.add(c);
			cnt++;
		}
		num++;
		
		jobToIncompleteMapper.put(jid, num);
		
		increase_ts(numMappers, num);
		
		/* read mapper class */
		Registry reg = LocateRegistry.getRegistry(jobHostname, registryPort);
		try {
			JobTrackerI jobtracker = (JobTrackerI)reg.lookup(jobServiceName);
			
			Pair mapper = jobtracker.readMapper(jid);
			
			Util.writeBinaryToFile(mapper.content, mapper.name + ".class");
			
			ProcessBuilder process = new ProcessBuilder();
			
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		/* new mapper instance and do job */
		
		/* store the intermediate result (mapper does this) */
		
		/* notify the job tracker if num of mapper reach zero */
		
	}


	@Override
	public void pushReduceTask(int jobid, 
			HashMap<String, String> files) throws RemoteException {
		/* store the intermediate file information */
		
		/* read the intermediate files*/
		
		/* read reducer class */
		
		/* store the final result */
		
		/* notify the job tracker */
	}
	
}
