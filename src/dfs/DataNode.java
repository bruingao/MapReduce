package dfs;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
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
	
	/* configuration file */
	private static String confPath = "conf/dfs.conf";
	
	/* datanode file */
	private static String dnPath = "conf/slaves";
	
	private static  Registry registry;
	
	/* stored files' replicas (filename + chunk number) */
	private static volatile HashSet<String> files
		= new HashSet<String>();
	
	/* datanodes' status */
	private static volatile HashSet<String> datanodes 
		= new HashSet<String>();
	
	/* all the files' path */
	private static String dataNodePath;
		
	private static String nameNodeHostname;
	
	private static Integer nameRegPort;
	private static Integer dataRegPort;
	
	private static Integer dataNodePort;
		
	private static String dataNodeServiceName;
	
	private static String nameNodeServiceName;
	
	public DataNode() throws RemoteException{
		
	}
	
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
	
	/* init work */
	private void Init() {
		Object obj = Util.readObject(dataNodePath+"files");
		if(obj != null)
			files = (HashSet<String>) obj;
		
//		obj = Util.readObject(dataNodePath + "datanodes");
//		if(obj != null)
//			datanodes = (HashSet<String>) obj;
//		else
//			Util.writeObject(dataNodePath+"datanodes", datanodes);
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
		
		System.out.println(content);
		System.out.println(temp);
		
		Util.writeBinaryToFile(content, temp);
		
		add_ts(files,filename);
		
		/* check point */
		Util.checkpointFiles(dataNodePath + "files", files);
//		Util.writeObject(dataNodePath + "files", files);
	}
	
	@Override
	public void removeFile(String filename) throws RemoteException {
				
		remove_ts(files,filename);
		
		/* check point */
		Util.checkpointFiles(dataNodePath + "files", files);

//		Util.writeObject(dataNodePath + "files", files);
	}

	public static void readDataNodes(String filename) throws UnsupportedEncodingException {
		String content = new String(Util.readFromFile(filename),"UTF-8");
		
		String lines[] = content.split("\n");
		for(int i = 0; i < lines.length; i++) {
			add_ts(datanodes,lines[i]);
		}	
	}
	
	@Override
	public boolean heartBeat() {
		return true;
	}
	
	@Override
	public boolean replication(String filename, String[] nodes)
			throws RemoteException {
		
		if(nodes.length <= 0)
			return false;
		
		int index = 0;
		
		try {
		
		    Registry dnRegistry=LocateRegistry.getRegistry(nodes[index],dataRegPort);
			DataNodeI datanode = (DataNodeI)dnRegistry.lookup(dataNodeServiceName);
			byte[] content = datanode.read(filename);
			this.write(filename, content);
			return true;
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String args[]) {
		
		try
	    {
			 DataNode datanode = new DataNode();
			 Util.readConfigurationFile(confPath, datanode);
			 readDataNodes(dnPath);
			 datanode.Init();
			 
			 if(!dataNodePath.endsWith("/")) {
				 dataNodePath += "/";
			 }
			 
			 unexportObject(datanode, false);
			 DataNodeI stub = (DataNodeI) exportObject(datanode, dataNodePort);
			 
			 //registry = LocateRegistry.getRegistry(registryHostname, registryPort);
			 registry = LocateRegistry.createRegistry(dataRegPort);
			 //NameNodeI namenode = (NameNodeI)registry.lookup(registryHostname+"/"+nameNodeServiceName);
			 
			 InetAddress address = InetAddress.getLocalHost();
			 
			 System.out.println(address.getHostAddress());
			 
			 registry.rebind(dataNodeServiceName, stub);
			 
			 //System.out.println(dataNodeServiceName);
			 //registry.rebind(dataNodeServiceName, stub);
			 
			 //System.out.println ("DataNode ready!");
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();

	    	System.out.println("Exception happend when running the Datanode!");
	    }
		
	}

}
