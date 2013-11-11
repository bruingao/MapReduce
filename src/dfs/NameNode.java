package dfs;

import java.io.UnsupportedEncodingException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Common.dfsScheduler;
import Common.Util;

public class NameNode extends UnicastRemoteObject implements NameNodeI{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7921414827247184085L;

	/* configuration file */
	private static String confPath = "conf/dfs.conf";
	
	/* datanode file */
	private static String dnPath = "conf/slaves";
	
	/* replication factor read from configuration file */
	private static Integer replicaFactor;
	
	/* service name read from configuration file */
	private static String nameNodeServiceName;
	
	/* registry hostname read from configuration file */
	private static String registryHostname;
	
	/* registry port number read from configuration file */
	private static Integer registryPort;
	
	/* the NameNode's port read from configuration file */
	private static Integer nameNodePort;
	
	/* the dataNode's port number */
	private static Integer dataNodePort;
		
	/* datanode service name */
	private static String dataNodeServiceName;
	
	/* namenode tmp file path */
	public static String nameNodePath;
	
	public static Registry registry;
	
	private static ExecutorService executor = Executors.newCachedThreadPool();
	
	public NameNode() throws RemoteException{
	}
	
	private void Init() {
		/* read checkpoint */
		Object obj = Util.readObject(nameNodePath+"files");
		if (obj != null)
			dfsScheduler.setFiles((ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>) obj);
		
		obj = Util.readObject(nameNodePath+"nodeToReplicas");
		if (obj != null)
			dfsScheduler.setNodeToReplicas((ConcurrentHashMap<String, HashSet<String>>) obj);
		
		obj = Util.readObject(nameNodePath+"tempfiles");
		if (obj != null)
			dfsScheduler.setTempFiles((ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>) obj);
	}
	
	
	/* delete a file */
	public void deleteFile(){}
	
	/* check data nodes */
	public void checkDataNodes(){
		for (String host : dfsScheduler.getStatus().keySet()) {
			checkThread ct = new checkThread(host, registryPort, dataNodeServiceName);
			ct.setOp(checkThread.OP.STATUS);
			executor.execute(ct);
		}
	}
	
	/* check file replication */
	public void checkreplication() {
		for (String name : dfsScheduler.getFiles().keySet()) {
			HashMap<Integer, HashSet<String>> map = dfsScheduler.getFiles().get(name);
			for (Integer i : map.keySet()) {
				int cnt  = 0;
				HashSet<String> candidates = new HashSet<String>();
				for (String datanode : map.get(i)) {
					if(dfsScheduler.getStatus().get(datanode)) {
						cnt++;
						candidates.add(datanode);
					}
				}
				if (cnt > replicaFactor) {

					String res[] = dfsScheduler.chooseHeavy(cnt-replicaFactor, (String[])candidates.toArray());
					
					for (String r : res) {
						checkThread t = new checkThread(r, registryPort, dataNodeServiceName);
						t.setFilename(name);
						t.setChunknumber(i);
						t.setOp(checkThread.OP.DELETE);
						executor.execute(t);
					}
				}
				else if (cnt < replicaFactor) {
					String res[] = dfsScheduler.chooseLight(replicaFactor - cnt, candidates);
					
					for (String r : res) {
						if(r == null)
							break;
						
						checkThread t = new checkThread(r, registryPort, dataNodeServiceName);
						t.setFilename(name);
						t.setChunknumber(i);
						t.setOp(checkThread.OP.WRITE);
						t.setNodes( (String[]) candidates.toArray());
						executor.execute(t);
					}
				}
			}
		}
	}
	
	/* add a new file */	
	@Override
	public HashMap<Integer, HashSet<String>> writeFile(String filename, int num)
			throws RemoteException {
		
		HashMap<Integer, HashSet<String>> res = dfsScheduler.createFile(filename, num, replicaFactor);
		
		if (res.size() > 0)
			/* check point */
			Util.writeObject(nameNodePath + "tempfiles", dfsScheduler.getTempFiles());
		
		return res;
	}

	@Override
	public HashMap<Integer, HashSet<String>> open(String filename)
			throws RemoteException {

		/* TaskTacker call this method to get the files' relica places */
		return dfsScheduler.getFile(filename);
	}

	@Override
	public void writeSucess(String filename, boolean res) throws RemoteException {
		if(res) {
			dfsScheduler.transferTemp(filename);
			Util.writeObject(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
			Util.writeObject(NameNode.nameNodePath + "nodeToReplicas", dfsScheduler.getNodeToReplicas());
		}
		else {
			dfsScheduler.deleteTemp(filename);
		}
		
		/* check point */
		Util.writeObject(NameNode.nameNodePath+"tempfiles", dfsScheduler.getTempFiles());
	}
	
	public static void readDataNodes(String filename) {
		String content = null;
		try {
			content = new String(Util.readFromFile(filename), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String lines[] = content.split("\n");
		for(int i = 0; i < lines.length; i++) {
			dfsScheduler.getStatus().put(lines[i], false);
			dfsScheduler.getNodeToReplicas().put(lines[i], new HashSet<String>());
		}
	}
	
	public void checkTimer() {
		Timer check = new Timer();
		check.scheduleAtFixedRate (new TimerTask(){
			
			@Override
			public void run() {
				checkDataNodes();
				checkreplication();
			}
		},0, 5000);
	}
	
	public static void main(String []args) {
		
		try
	    {
			 NameNode server = new NameNode();
			 Util.readConfigurationFile(confPath, server);
			 			 
			 readDataNodes(dnPath);
			 server.Init();

			 unexportObject(server, false);
			 NameNodeI stub = (NameNodeI) exportObject(server, nameNodePort);
			 
			 registry = LocateRegistry.createRegistry(registryPort);
			 //registry.rebind(registryHostname + "/" + nameNodeServiceName, stub);
			 registry.rebind(nameNodeServiceName, stub);
			 
			 System.out.println ("NameNode ready!");
			 
			 server.checkTimer();
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    }
	}

	@Override
	public ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> listFiles() throws RemoteException {
		return dfsScheduler.getFiles();
	}

	@Override
	public ConcurrentHashMap<String, HashSet<String>> listNodes()
			throws RemoteException {
		return dfsScheduler.getNodeToReplicas();
	}

	@Override
	public void removeFile(String filename) throws RemoteException {
		dfsScheduler.removeFile(filename);		
		
		Util.writeObject(nameNodePath+"files", dfsScheduler.getFiles());
		Util.writeObject(nameNodePath + "nodeToReplicas", dfsScheduler.getNodeToReplicas());
	}
    /*
    @Override	
	public void proxyRebind(String dataNodeServiceName, DataNodeI datanode) throws RemoteException {
	    try
	    {
            registry.rebind(dataNodeServiceName, datanode);
        }
        catch (Exception e)
	    {
	    	e.printStackTrace();
	    }
	}
	*/
		
}
