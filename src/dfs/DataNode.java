package dfs;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;

import Common.Util;

public class DataNode extends UnicastRemoteObject implements DataNodeI{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2961863470847180775L;
	
	/* stored files' replicas (filename + chunk number) */
	private static volatile HashSet<String> files
		= new HashSet<String>();
	
	/* datanodes' status */
	private static volatile HashSet<String> datanodes 
		= new HashSet<String>();
	
	/* all the files' path */
	private static String dataNodePath;
		
	private static String registryHostname;
	
	private static int registryPort;
	
	private static int dataNodePort;
	
	private static String dataNodeServiceName;
	
	private static void add_ts(HashSet<String> obj, String filename) {
		synchronized(obj) {
			files.add(filename);
		}
	}
	
	private static void remove_ts(HashSet<String> obj, String filename) {
		synchronized(obj) {
			files.remove(filename);
		}
	}
	
	public DataNode() throws RemoteException{
		Init();
	}
	
	/* init work */
	private void Init() {
		Object obj = Util.readObject(dataNodePath+"files");
		if(obj != null)
			files = (HashSet<String>) obj;
		
		obj = Util.readObject(dataNodePath + "datanodes");
		if(obj != null)
			datanodes = (HashSet<String>) obj;
	}
	
	/* read the corresponding chunk according to the filename */
	@Override
	public byte[] read(String filename) {
		
		String temp = dataNodePath + filename;
		
		return Util.readFromFile(temp);
	}
	
	/* write the corresponding chunk */
	@Override
	public void write(String filename, byte[] content) {
		
		String temp = dataNodePath + filename;
		
		Util.writeBinaryToFile(content, temp);
		
		add_ts(files,filename);
	}
	
	@Override
	public void removeFile(String filename) throws RemoteException {
				
		remove_ts(files,filename);
		
	}

	public static void readDataNodes(String filename) {
		String content = Util.readFromFile(filename).toString();
		String lines[] = content.split("\n");
		for(int i = 0; i < lines.length; i++) {
			add_ts(datanodes,lines[i]);
		}
	}
	
	@Override
	public boolean heartBeat() {
		return true;
	}
	
	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("Usage: DataNode [configuration file name] [data nodes info. file]");
			System.exit(0);
		}
		
		try
	    {
			 DataNode datanode = new DataNode();
			 Util.readConfigurationFile(args[1], datanode);
			 
			 DataNodeI stub = (DataNodeI) exportObject(datanode, dataNodePort);
			 
			 Registry registry = LocateRegistry.getRegistry(registryHostname, registryPort);
			 registry.bind(dataNodeServiceName, stub);
			 
			 System.out.println ("NameNode ready!");
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	    }
		
	}

}
