package mapred;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.SortedMap;

import dfs.DataNodeI;
import dfs.NameNodeI;

import format.outputFormat;

import Common.Collector;
import Common.Pair;
import Common.Util;

public class ReduceRunner {
	
	private static Reducer reducer;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		/* reducer class name */
		String classname = args[0];
		
		/* job id */
		int jid = Integer.parseInt(args[1]);
		
		/* output file name */
		String outputFilename = args[2];
		
		/* output format class name */
		String outputformat = args[3];
		
		/* datanode registry port */
		int dataRegPort = Integer.parseInt(args[4]);
		
		/* datanode registry servicename */
		String dataService = args[5];
		
		/* tasktracker registry port */
		int taskRegPort = Integer.parseInt(args[6]);
		
		/* tasktracker registry servicename */
		String taskService = args[7];
		
		/* namenode registry port */
		int nameRegPort = Integer.parseInt(args[8]);
		
		/* namenode registry service name */
		String nameService = args[9];
		
		/* namenode host name */
		String nameHost = args[10];
		
		/* partition number */
		int partition = Integer.parseInt(args[11]);
		
		/* file chunk size */
		int chunksize = Integer.parseInt(args[12]);
		
		/* intermediate nodes size */
		int nSize = Integer.parseInt(args[13]);
		
		/* all the nodes having intermediate files */
		String nodes[] = new String[nSize];
		
		for(int i = 0; i <nSize; i++) {
			nodes[i] = args[14 + i];
		}
		
		Collector collector = new Collector();
		
		try {
			Class<Reducer> rd = (Class<Reducer>) Class.forName(classname);
			
			Constructor<Reducer> constructor = rd.getConstructor();
			
			reducer = constructor.newInstance();
			
			String contents[] = new String[nSize];
			
			for(int i = 0; i < nSize; i++){
				/* read intermediate file */
				Registry reg = LocateRegistry.getRegistry(nodes[i], taskRegPort);
				
				TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(taskService);
				
				contents[i] = tasktracker.getInterFiles(jid, partition);
				
//				System.out.println("content file "+i+": "+contents[i]);
				
				/* if read error return -1, reducer fail */
				if(contents[i] == null)
					System.exit(-1);
			}
			
			ArrayList<HashMap<String, ArrayList<String>>> kvPairs 
				= new ArrayList<HashMap<String, ArrayList<String>>>();
			
			/* do reduce job */
			for (String content : contents) {
				if(content.equals("") || content == null)
					continue;
				kvPairs.add(Util.parseStr(content));
			}
			
			if(kvPairs.size() == 0)
				return;
			
			ArrayList<HashMap<String, ArrayList<String>>> newPairs 
			= new ArrayList<HashMap<String, ArrayList<String>>>();
			while (kvPairs.size()>1) {
				int i;
				for(i = 0;i*2+1 < kvPairs.size(); i++) {
					newPairs.add(Util.mergeArray(kvPairs.get(i*2), kvPairs.get(i*2+1)));
				}
				if(i*2+1 == kvPairs.size()) {
					newPairs.add(kvPairs.get(i*2));
				}
				kvPairs = newPairs;
				newPairs = new ArrayList<HashMap<String, ArrayList<String>>>();
			}
			
			HashMap<String, ArrayList<String>> values = kvPairs.get(0);
			
			for(String key : values.keySet()) {
				reducer.reduce(key, values.get(key), collector);
			}
			
			
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (NotBoundException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
		
		/* transform the computed partition to a string, ready to write to dfs */
		outputFormat output = new outputFormat(collector.collection);
		
		byte[] result = null;
		try {
			result = output.getOutput().toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		ArrayList<Integer> range = new ArrayList<Integer>();
		
		int chunknum = Util.decideChunkNumber(result.length, chunksize, range, result);
		
		String newName = jid + "-" + outputFilename+"part-"+partition;
		
		/* open name node */
		Hashtable<Integer, HashSet<String>> newFile = null;
		NameNodeI namenode = null;
		
		try {
			Registry reg = LocateRegistry.getRegistry(nameHost, nameRegPort);
			namenode = (NameNodeI) reg.lookup(nameService);
			newFile = namenode.writeFile(newName, chunknum);
			
			if (newFile == null) {
				System.out.println("The filename " +newName+ " is taken!Please choose another name!");
				namenode.writeSucess(newName, false);
				System.exit(1);
			}
			
			if (newFile.size() == 0) {
				System.out.println("There is no enough nodes for storing the data!");
				namenode.writeSucess(newName, false);
				System.exit(1);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (NotBoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		for(int chunk : newFile.keySet()) {
			int count = 0;
			for (String dnode : newFile.get(chunk)){
			    Registry dreg;
				try {
					dreg = LocateRegistry.getRegistry(dnode, dataRegPort);
					DataNodeI datanodeI = (DataNodeI)dreg.lookup(dataService);
					datanodeI.write(newName+"-"+chunk, Arrays.copyOfRange(result, range.get(chunk), range.get(chunk+1)));
					count++;
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				}
			}
			if(count <= 0) {
				try {
					namenode.writeSucess(newName, false);
				} catch (RemoteException e) {
					e.printStackTrace();
					System.exit(1);
				}
				System.exit(1);
			}
		}
		
		try {
			namenode.writeSucess(newName, true);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
