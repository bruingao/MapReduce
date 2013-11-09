package bin;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import dfs.DataNodeI;
import dfs.NameNodeI;

import Common.Util;

public class MRManager {	
	
	/* configuration file */
	private static String confName = "../conf/dfs.conf";
	
	/* service name read from configuration file */
	private static String nameNodeServiceName;
	
	/* registry hostname read from configuration file */
	private static String registryHostname;
	
	/* registry port number read from configuration file */
	private static int registryPort;
	
	/* the NameNode's port read from configuration file */
	private static int nameNodePort;
	
	/* name Node host name */
	private static String nameNodeHostname;
	
	/* data node port number */
	private static int dataNodePort;
	
	/* data node service name */
	private static String dataNodeServiceName;
	
	/* chunk size */
	private static int chunksize;
	
	private void doDfs(String[] cmds) throws RemoteException, Exception {
		Registry registry;
		NameNodeI namenode = null;
		
		registry = LocateRegistry.getRegistry(registryHostname, registryPort);
		namenode = (NameNodeI)registry.lookup(nameNodeServiceName);
		
		String cmd = cmds[2];
		
		switch(cmd) {
			case "listfile":
				ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> files = namenode.listFiles();
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
			case "import":
				if(cmds.length < 4) {
					System.out.println("Please indicate the input file name");
					break;
				}
				else if (cmds.length < 5) {
					System.out.println("Please indicate the file name");
					break;
				}
				
				String inputname = cmds[3];
				String filename = cmds[4];
				
				int linesize = 0;
				byte[] content = Util.readFromFile(inputname);
				while(true) {
					if (content[linesize] == '\n')
						break;
					linesize++;
				}
	
				linesize++;
				
				int size = content.length;
				
				/* real chunk size similar to chunksize but round to avoid partial line in a chunk */
				int csize = chunksize/linesize*linesize;
				
				int chunknumber = (size + csize - 1)/csize;
				
				HashMap<Integer,HashSet<String>> res 
					= namenode.writeFile(filename, chunknumber);
				
				if (res == null) {
					System.out.println("The filename" +filename+ " is taken!Please choose another name!");
					break;
				}
				
				for(int chunk : res.keySet()) {
					for (String dnode : res.get(chunk)){
						DataNodeI datanodeI = (DataNodeI)registry.lookup("rmi://"+dnode+":"+dataNodePort+"/"+dataNodeServiceName);
						datanodeI.write(filename+chunk, Arrays.copyOfRange(content, chunk * csize, (chunk+1)*csize));
					}
				}
				
				namenode.writeSucess(filename, true);
				
				break;
				
			case "remove":
				if(cmds.length < 4) {
					System.out.println("Please indicate the file name");
					break;
				}
				
				String fname = cmds[3];
				
				HashMap<Integer, HashSet<String>> file
					= namenode.open(fname);
				
				if (file == null) {
					System.out.println("There is no such file: "+fname);
					break;
				}
				
				for(int chunk : file.keySet()) {
					for (String dnode : file.get(chunk)){
						DataNodeI datanodeI = (DataNodeI)registry.lookup("rmi://"+dnode+":"+dataNodePort+"/"+dataNodeServiceName);
						datanodeI.removeFile(fname + chunk);
					}
				}
				
				namenode.removeFile(fname);
				break;
		}
	}
	
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: MRManager [dfs or jobtracker] [Job Name] <other arguments>");
			System.exit(0);
		}
		
		MRManager server = new MRManager();
		
		Util.readConfigurationFile(confName, server);
		
		switch(args[1]) {
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
