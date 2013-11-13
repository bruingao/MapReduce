package dfs;

import java.io.*;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;

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
		
	private static String registryHostname;
	
	private static Integer registryPort;
	
	private static Integer dataNodePort;
		
	private static String dataNodeServiceName;
	
	private static String nameNodeServiceName;
	
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
		
		Util.writeBinaryToFile(content, temp);
		
		add_ts(files,filename);
		
		/* check point */
		Util.writeObject(dataNodePath + "files", files);
	}
	
	@Override
	public void removeFile(String filename) throws RemoteException {
				
		try{
    		File file = new File(dataNodePath+filename);
    		file.setWritable(true);
    		if(file.delete()){
    			System.out.println(file.getName() + " is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    	}catch(Exception e){
    		e.printStackTrace();
    	}

		remove_ts(files,filename);
		
		
		/* check point */
		Util.writeObject(dataNodePath + "files", files);
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
		Date date = new Date();
		Random rand = new Random(date.getTime());
		
		int index = rand.nextInt()%nodes.length;
		
		try {
		
		    Registry dnRegistry=LocateRegistry.getRegistry(nodes[index],registryPort);
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

			 unexportObject(datanode, false);
			 DataNodeI stub = (DataNodeI) exportObject(datanode, dataNodePort);
			 
			 //registry = LocateRegistry.getRegistry(registryHostname, registryPort);
			 registry = LocateRegistry.createRegistry(registryPort);
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
	    	System.out.println("Exception happend when running the Datanode!");
	    }
		
	}

}
