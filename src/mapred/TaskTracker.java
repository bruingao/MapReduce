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
	
	/* job tracker's registry */
	private static Registry registry;
	
	/* data dfs node path */
	private static String dataNodePath;
	
	/* minimun chunks */
	private static Integer minChunk;
	
	/* jobid to filename and corresponding chunks */
	private static ConcurrentHashMap<Integer, HashMap<String, HashSet<String>>> jobToInput
		= new ConcurrentHashMap<Integer, HashMap<String, HashSet<String>>>();
	
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
	
	private static Integer numMappers = 0;
	private static Integer numReducers = 0;
	
	
	private void increase_ts(Integer t) {
		synchronized(t) {
			t++;
		}
	}
	private void decrease_ts(Integer t) {
		synchronized(t) {
			t--;
		}
	}
	
	protected TaskTracker() throws RemoteException {
		super();
	}

	@Override
	public void pushMapTask(int jid, HashMap<Integer, HashSet<String>> filereplicas,
			String filename, HashSet<Integer> chunks) throws RemoteException {
		/* store the files information in jobs and replicas */
		
		//String content = "";
		
		
//		/* I think should let mapper do this */
//		
//		/* read files */
//		if(filereplicas != null) {
//			for (int i : chunks) {
//				for (String dnode : filereplicas.get(i)) {
//					Registry reg = LocateRegistry.getRegistry(dnode, dataNodePort);
//					try {
//						DataNodeI datanode = (DataNodeI)reg.lookup(dataNodeServiceName);
//						content.concat(new String(datanode.read(filename + i),"UTF-8"));
//					} catch (NotBoundException e) {
//						e.printStackTrace();
//					} catch (UnsupportedEncodingException e) {
//						e.printStackTrace();
//					}
//				}
//			}
//		} else {
//			for (int i : chunks) {
//				try {
//					content.concat(new String(Util.readFromFile(dataNodePath+filename+i), "UTF-8"));
//				} catch (UnsupportedEncodingException e) {
//					e.printStackTrace();
//				}
//			}
//		}
		
		/* queue */
		if (numMappers >= maxMappers) {
			HashMap<String, HashSet<String>> fileToReplicas = new HashMap<String, HashSet<String>>();
			for(int i : chunks) {
				fileToReplicas.put(filename+i, filereplicas.get(i));
			}
			jobToInput.put(jid, fileToReplicas);
			return;
		}
		
		/* distributed chunks */
		int cnt = 0;
		int num = 0;
		HashSet<HashSet<Integer>> pcks = new HashSet<HashSet<Integer>>();
		HashSet<Integer> cks = null;
		for (int c : chunks) {
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
		
		/* read mapper class */
		
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
