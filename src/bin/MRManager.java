package bin;

import java.io.*;
import java.util.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import dfs.DataNodeI;
import dfs.NameNodeI;

import Common.Util;

public class MRManager {	
	
	/* configuration file */
	private static String confName = "conf/dfs.conf";
	
	/* service name read from configuration file */
	private static String nameNodeServiceName;
	
	/* registry hostname read from configuration file */
	private static String nameNodeHostname;
	
	/* registry port number read from configuration file */
	private static Integer nameRegPort;
	private static Integer dataRegPort;
	
	/* data node service name */
	private static String dataNodeServiceName;
	
	/* chunk size */
	private static Integer chunksize;
	
	
	
	private void doDfs(String[] cmds) throws RemoteException, Exception {
		Registry registry;
		NameNodeI namenode = null;
		
		registry = LocateRegistry.getRegistry(nameNodeHostname, nameRegPort);
		//namenode = (NameNodeI)registry.lookup(registryHostname+"/"+nameNodeServiceName);
		namenode = (NameNodeI)registry.lookup(nameNodeServiceName);
		
		String cmd = cmds[1];
		
		switch(cmd) {
			case "listfile":
				ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> files = namenode.listFiles();
				for(String file : files.keySet()) {
					System.out.println(file+":");
					for (int chunk : files.get(file).keySet()) {
						System.out.print("chunk"+chunk+":");
						for (String node : files.get(file).get(chunk)) {
							System.out.print(node+" ");
						}
						System.out.println();
					}
				}
				break;
			case "listnode":
				ConcurrentHashMap<String, HashSet<String>>  nodes = namenode.listNodes();
				for(String node : nodes.keySet()) {
					System.out.println(node+":");
					for (String file : nodes.get(node)) {
						System.out.print(file+" ");
					}
					System.out.println();
				}
				break;
		    case "export":
		        if(cmds.length<3){
		            System.out.println("Please indicate the file name");
					break;
		        }
		        String theFilename = cmds[2];
		        
//		        ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> theFiles = namenode.listFiles();
		        Hashtable<Integer, HashSet<String>> filechunks = namenode.open(theFilename);
//		        Hashtable<Integer, HashSet<String>> filechunks = theFiles.get(theFilename);
		        if(filechunks == null) {
		        	System.out.println("The file does not exist!");
		        	break;
		        }
		        
		        System.out.println("num of chunks: "+filechunks.size());
		        
		        SortedSet<Integer> chunkNums = new TreeSet<Integer>(filechunks.keySet());
		        
		        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		        for (Integer chunknum : chunkNums){
		            String dnode=filechunks.get(chunknum).iterator().next();
		            registry = LocateRegistry.getRegistry(dnode, dataRegPort);
					DataNodeI datanodeI = (DataNodeI)registry.lookup(dataNodeServiceName);
					byte[] chunkContent = datanodeI.read(theFilename+"-"+chunknum);
					byteStream.write(chunkContent);
		        }
                byte[] fileContent=byteStream.toByteArray();
                Util.writeBinaryToFile(fileContent,theFilename);
		        
				break;
			case "import":
				if(cmds.length < 3) {
					System.out.println("Please indicate the input file name");
					break;
				}
				else if (cmds.length < 4) {
					System.out.println("Please indicate the file name");
					break;
				}
				
				String inputname = cmds[2];
				String filename = cmds[3];
				
				byte[] content = Util.readFromFile(inputname);
				
				if(content == null)
					break;
				
				int size = content.length;
				
				ArrayList<Integer> range = new ArrayList<Integer>();	
				
				int chunknumber = Util.decideChunkNumber(size, chunksize, range, content);
				
				Hashtable<Integer,HashSet<String>> res 
					= namenode.writeFile(filename, chunknumber);
				
				if (res == null) {
					System.out.println("The filename " +filename+ " is taken!Please choose another name!");
					namenode.writeSucess(filename, false);
					break;
				}
				
				if (res.size() == 0) {
					System.out.println("There is no enough nodes for storing the data!");
					namenode.writeSucess(filename, false);
					break;
				}
				
				for(int chunk : res.keySet()) {
					for (String dnode : res.get(chunk)){
					
					    registry = LocateRegistry.getRegistry(dnode, dataRegPort);
						DataNodeI datanodeI = (DataNodeI)registry.lookup(dataNodeServiceName);
						datanodeI.write(filename+"-"+chunk, Arrays.copyOfRange(content, range.get(chunk), range.get(chunk+1)));
					}
				}
				
				namenode.writeSucess(filename, true);
				
				break;
				
			case "remove":
				if(cmds.length < 3) {
					System.out.println("Please indicate the file name");
					break;
				}
				
				String fname = cmds[2];
				
				Hashtable<Integer, HashSet<String>> file
					= namenode.open(fname);
				
				if (file == null) {
					System.out.println("There is no such file: "+fname);
					break;
				}
				
				for(int chunk : file.keySet()) {
					for (String dnode : file.get(chunk)){
					    registry = LocateRegistry.getRegistry(dnode, dataRegPort);
						DataNodeI datanodeI = (DataNodeI)registry.lookup(dataNodeServiceName);
						datanodeI.removeFile(fname + "-" +chunk);
					}
				}
				
				namenode.removeFile(fname);
				break;
		}
	}
	
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: MRManager [dfs or jobtracker] [Job Name] <other arguments>");
			System.exit(0);
		}
		
		MRManager server = new MRManager();
		
		Util.readConfigurationFile(confName, server);
		
		switch(args[0]) {
			case "dfs":
				try {
					server.doDfs(args);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			case "jobtracker":
				break;
			default:
				break;
		}
	}
}