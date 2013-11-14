package mapred;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import Common.Util;

import mapred.JobTracker.NOTIFY_RESULT;

public class taskMapThread implements Runnable{

	private int jid;
	private int numOfChunks;
	private JobConf conf;
	private ArrayList<Integer> chunks;
	private ArrayList<String> dataNodeHost;
	private int regPort;
	private String serviceName;
	
	private String partitionPath;
	private int numPartitions;
	
	private String classname;
	
	public taskMapThread(int jid, int numOfChunks, JobConf conf,
			ArrayList<Integer> chunks, ArrayList<String> dataNodeHost, int regPort,
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
		
//		List<String> commands = new ArrayList<String>();
		
		
		/* Why we transfer all the parameters to maprunner but not let the runner read configuration file 
		 * is because of performance concern. (Disk read is expensive and a lot of unrelated parameters) */
		String cmd = "java "+ "mapred.MapRunner" + " " + this.classname + " " + numPartitions + " " + partitionPath 
				+ " " +jid + " " + numOfChunks
				+ " " + conf.getInputfile() + " " + conf.getInputFormat().getName() + " " ;
		
		if(chunks==null)
			System.out.println("null chunks");
		
		for (int c : chunks) {
			cmd += c + " ";
		}
		
		for (String node : dataNodeHost) {
			cmd += node+" ";
		}
		
		cmd +=  regPort + " " + serviceName;
		
		System.out.println(cmd);
		
//		for(String s : cmd.split(" "))
//			commands.add(s);
		
//		ProcessBuilder process = new ProcessBuilder();
//		
//		process.command(commands);
		
		JobTrackerI jobtracker = null;
				
		try {
			
			int exitStatus = Util.buildProcess(cmd);
			
//			System.out.println("before start");
//			process.inheritIO();
//			Process task = process.start();
//						
////			StreamGobbler errorGobbler = new StreamGobbler(task.getErrorStream(), "ERROR");
////			StreamGobbler outputGobbler = new StreamGobbler(task.getInputStream(), "OUTPUT");
//			
////			errorGobbler.start();
////			outputGobbler.start();
//			
//			System.out.println("after start");
//			
//			int exitStatus = task.waitFor();
			
			//InputStream str = task.getInputStream();
			//String response = Util.convertStreamToStr(str);
			//String response = "for test";
			
			System.out.println("exitcode: "+exitStatus);
			
			Registry reg = LocateRegistry.getRegistry(TaskTracker.jobHostname, TaskTracker.jobRegPort);
			jobtracker = (JobTrackerI)reg.lookup(TaskTracker.jobServiceName);
			
			if(TaskTracker.jobToIncompleteMapper.get(jid) != null) {
				/* process response and exit status */
				if(exitStatus != 0) {
					if (exitStatus == -1)
						jobtracker.notifyMapResult(JobTracker.NOTIFY_RESULT.FAIL,
								jid, TaskTracker.hostAddress);
					else
						jobtracker.notifyMapResult(JobTracker.NOTIFY_RESULT.DATANODE_FAIL, 
								jid, TaskTracker.hostAddress);
					
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
					
					System.out.println(TaskTracker.jobToIncompleteMapper.get(jid));
					
					TaskTracker.jobToIncompleteMapper.put(jid, TaskTracker.jobToIncompleteMapper.get(jid)-1);
					
					System.out.println(TaskTracker.jobToIncompleteMapper.get(jid));
					
					if (TaskTracker.jobToIncompleteMapper.get(jid) <= 0) {
						jobtracker.notifyMapResult(JobTracker.NOTIFY_RESULT.SUCCESS, jid, TaskTracker.hostAddress);
						TaskTracker.jobToIncompleteMapper.remove(jid);
						System.out.println("Job "+jid +"Succeed!");
					}
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			if(jobtracker != null) {
				try {
					jobFail(jobtracker);
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
			try {
				jobFail(jobtracker);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		} catch (NotBoundException e) {
			e.printStackTrace();
			try {
				jobFail(jobtracker);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}
	}
	
	/* if caused by exceptions in this thread */
	private void jobFail(JobTrackerI jobtracker) throws RemoteException {
		System.out.println("Job "+jid +"Failed!");
		TaskTracker.jobToIncompleteMapper.remove(jid);
		TaskTracker.jobToInter.remove(jid);
		jobtracker.notifyMapResult(JobTracker.NOTIFY_RESULT.FAIL, jid, TaskTracker.hostAddress);
	}
	
}
