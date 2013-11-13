package mapred;

import java.io.IOException;
import java.io.InputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;

import Common.Util;

public class taskThread implements Runnable{

	private int jid;
	private int numOfChunks;
	private JobConf conf;
	private HashSet<Integer> chunks;
	private HashSet<String> dataNodeHost;
	private int regPort;
	private String serviceName;
	
	private String partitionPath;
	private int numPartitions;
	
	private String classname;
	
	public taskThread(int jid, int numOfChunks, JobConf conf,
			HashSet<Integer> chunks, HashSet<String> dataNodeHost, int regPort,
			String serviceName, String classname, String partitionPath, int numPartitions) {
		super();
		this.jid = jid;
		this.numOfChunks = numOfChunks;
		this.conf = conf;
		this.chunks = chunks;
		this.dataNodeHost = dataNodeHost;
		this.regPort = regPort;
		this.serviceName = serviceName;
		this.classname = classname;
		this.partitionPath = partitionPath;
		this.numPartitions = numPartitions;
	}
	
	@Override
	public void run(){
		
		String cmd = "java "+ "MapRunner" + " " + this.classname + " " + numPartitions + " " + partitionPath 
				+ " " +jid + " " + numOfChunks
				+ " " + conf.getInputfile() + " " + conf.getInputFormat().getName() + " " ;
		
		for (int c : chunks) {
			cmd += c + " ";
		}
		
		cmd += dataNodeHost + " " + regPort + " " + serviceName;
		
		System.out.println(cmd);
		
		ProcessBuilder process = new ProcessBuilder(cmd);
		
		JobTrackerI jobtracker = null;
				
		try {
			Process task = process.start();
			InputStream str = task.getInputStream();
			int exitStatus = task.waitFor();
			String response = Util.convertStreamToStr(str);
			
			Registry reg = LocateRegistry.getRegistry(TaskTracker.jobHostname, TaskTracker.taskRegPort);
			jobtracker = (JobTrackerI)reg.lookup(TaskTracker.jobServiceName);
			
			if(TaskTracker.jobToIncompleteMapper.get(jid) != null) {
				/* process response and exit status */
				if(exitStatus != 0) {
					jobtracker.notifyMapResult(false, jid, TaskTracker.hostAddress);
					TaskTracker.jobToIncompleteMapper.remove(jid);
					TaskTracker.jobToInter.remove(jid);
					System.out.println("Job "+jid +"Failed!");
				} else {
//					String files[] = response.split("\n");
					
					HashSet<String[]> temp = TaskTracker.jobToInter.get(jid);
					
					if(temp == null)
						temp = new HashSet<String[]>();
					
					String partitions[] = new String[numPartitions];
					String suffix = "-" + chunks.toArray()[0].toString();
					for(int i = 0; i < numPartitions; i++) {
						partitions[i] = jid+"partition"+i+suffix;
												
						System.out.println(partitions[i]);
					}	
					
					temp.add(partitions);
					
					TaskTracker.jobToInter.put(jid, temp);
					
					TaskTracker.decrease_ts(TaskTracker.numMappers, 1);
					TaskTracker.jobToIncompleteMapper.put(jid, TaskTracker.jobToIncompleteMapper.get(jid)-1);
					if (TaskTracker.jobToIncompleteMapper.get(jid) == 0) {
						jobtracker.notifyMapResult(true, jid, TaskTracker.hostAddress);
						TaskTracker.jobToIncompleteMapper.remove(jid);
					}
					
					System.out.println("Job "+jid +"Succeed!");
				}
			}
			
		} catch (IOException e) {
			if(jobtracker != null) {
				try {
					jobFail(jobtracker);
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		} catch (InterruptedException e) {
			try {
				jobFail(jobtracker);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		} catch (NotBoundException e) {
			try {
				jobFail(jobtracker);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}
	}
	
	private void jobFail(JobTrackerI jobtracker) throws RemoteException {
		System.out.println("Job "+jid +"Failed!");
		TaskTracker.jobToIncompleteMapper.remove(jid);
		TaskTracker.jobToInter.remove(jid);
		jobtracker.notifyMapResult(false, jid, TaskTracker.hostAddress);
	}

	
}
