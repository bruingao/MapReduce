package dfs;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import Common.Scheduler;
import Common.Util;

public class NameNode extends UnicastRemoteObject implements NameNodeI{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7921414827247184085L;

	/* replication factor read from configuration file */
	private static int replicaFactor;
	
	/* service name read from configuration file */
	private static String NameNodeServiceName;
	
	/* registry hostname read from configuration file */
	private static String registryHostname;
	
	/* registry port number read from configuration file */
	private static int registryPort;
	
	/* the NameNode's port read from configuration file */
	private static int nameNodePort;
	
	/* chunk size */
	private static int chunksize;	
	
	/* namenode tmp file path */
	private static String nameNodePath;

	
	public NameNode() throws RemoteException{
		Init();
	}
	
	private void Init() {
		Object obj = Util.readObject(nameNodePath+"files");
		if (obj != null)
			Scheduler.setFiles((ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>) obj);
		
		obj = Util.readObject(nameNodePath+"nodeToReplicas");
		if (obj != null)
			Scheduler.setNodeToReplicas((ConcurrentHashMap<String, HashSet<String>>) obj);
		
		obj = Util.readObject(nameNodePath+"status");
		if (obj != null)
			Scheduler.setStatus((ConcurrentHashMap<String, Boolean>) obj);
		
		obj = Util.readObject(nameNodePath+"tempfiles");
			Scheduler.setTempFiles((ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>) obj);
	}
	
	
	/* delete a file */
	public void deleteFile(){}
	
	/* check data nodes */
	public void checkDataNodes(){
		
		for()
		Registry registry = LocateRegistry.getRegistry(host);
        Hello stub = (Hello) registry.lookup("Hello");
	}

	/* add a new file */	
	@Override
	public HashMap<Integer, HashSet<String>> writeFile(String filename, int lines)
			throws RemoteException {
		
		return Scheduler.createFile(filename, lines%chunksize>0? lines/chunksize + 1:lines/chunksize, replicaFactor);
	}

	@Override
	public HashMap<Integer, HashSet<String>> open(String filename)
			throws RemoteException {

		/* TaskTacker call this method to get the files' relica places */
		return Scheduler.getFile(filename);
	}

	@Override
	public void writeSucess(String filename, boolean res) throws RemoteException {
		if(res)
			Scheduler.transferTemp(filename);
		else
			Scheduler.deleteTemp(filename);
	}
	
	public static void readDataNodes(String filename) {
		String content = Util.readFromFile(filename).toString();
		String lines[] = content.split("\n");
		for(int i = 0; i < lines.length; i++) {
			Scheduler.getStatus().put(lines[i], true);
			Scheduler.getNodeToReplicas().put(lines[i], null);
		}
	}
	
	public static void main(String []args) {
		
		if (args.length < 3) {
			System.out.println("Usage: NameNode [configuration file name] [datanodes info. file name]");
			System.exit(0);
		}
		
		
		try
	    {
			 NameNode server = new NameNode();
			 Util.readConfigurationFile(args[1], server);
			 readDataNodes(args[2]);
			 
			 NameNodeI stub = (NameNodeI) exportObject(server, nameNodePort);
			 
			 Registry registry = LocateRegistry.getRegistry(registryHostname, registryPort);
			 registry.bind(NameNodeServiceName, stub);
			 
			 System.out.println ("NameNode ready!");
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    }
	}
		
}
