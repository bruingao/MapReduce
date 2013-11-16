package mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.SortedSet;
import java.util.TreeSet;

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
		
		/* intermediate file path */
		String interPath = args[14];
		
		/* all the nodes having intermediate files */
		String nodes[] = new String[nSize];
		
		for(int i = 0; i <nSize; i++) {
			nodes[i] = args[15 + i];
		}
		
		Collector collector = new Collector();
		
		try {
			Class<Reducer> rd = (Class<Reducer>) Class.forName(classname);
			
			Constructor<Reducer> constructor = rd.getConstructor();
			
			reducer = constructor.newInstance();
						
			ArrayList<HashMap<String, ArrayList<String>>> kvPairs 
			= new ArrayList<HashMap<String, ArrayList<String>>>();
			
			for(int i = 0; i < nSize; i++){
				/* read intermediate file */
				Registry reg = LocateRegistry.getRegistry(nodes[i], taskRegPort);
				
				TaskTrackerI tasktracker = (TaskTrackerI) reg.lookup(taskService);
				
				String content = tasktracker.getInterFiles(jid, partition);
				
				
				Util.writeBinaryToFile(content.getBytes("UTF-8"), interPath+jid+"-tasktracker"+"-"+i+"-"+partition);

			}
			
			String currentLine[] = new String[nSize];
			String values[] = new String[nSize];
			SortedSet<String> orders = new TreeSet<String>();
			
			InputStreamReader isr[] = new InputStreamReader[nSize];
			BufferedReader br[] = new BufferedReader[nSize];
			FileInputStream in[] = new FileInputStream[nSize];
			for(int i = 0;i<nSize;i++) {
				in[i] = new FileInputStream(interPath+jid+"-tasktracker"+"-"+i+"-"+partition);
				isr[i] = new InputStreamReader(in[i]);
				br[i] = new BufferedReader(isr[i]);
				
				String temp = br[i].readLine();
				if (temp==null)
					continue;
				String lines[] = temp.split(" ");
				currentLine[i] = lines[0];
				values[i] = lines[1];
				orders.add(currentLine[i]);
			}
			
			if(orders.size()==0)
				return;
			
			while(true) {
				if(orders.size()==0)
					break;
				ArrayList<String> list = new ArrayList<String>();
				String minStr = orders.first();
				
				System.out.println(partition+": "+minStr);
				for (int i = 0;i<nSize;i++) {
					System.out.println(i+": "+currentLine[i]);
				}
				
				for (int i = 0;i<nSize;i++){
					while (currentLine[i] != null && currentLine[i].equals(minStr)) {
						list.add(values[i]);
						String temp = br[i].readLine();
						if (temp != null) {
							String lines[] = temp.split(" ");
							currentLine[i] = lines[0];
							values[i] = lines[1];
						} else {
							currentLine[i] = null;
						}
					}
				}
				orders.remove(minStr);
				for (int i = 0;i<nSize;i++){
					if(currentLine[i] == null)
						continue;
					orders.add(currentLine[i]);
				}
				if(list.size() > 0)
					reducer.reduce(minStr, list, collector);
			}
			


			
//			for(int i = 0)
//			if(content.equals("") || content == null)
//				continue;
//			
//			kvPairs.add(Util.parseStr(content));
			
			/* do reduce job */
//			for (String content : contents) {
//				if(content.equals("") || content == null)
//					continue;
//				kvPairs.add(Util.parseStr(content));
//			}
			
//			if(kvPairs.size() == 0)
//				return;
//			
//			ArrayList<HashMap<String, ArrayList<String>>> newPairs 
//			= new ArrayList<HashMap<String, ArrayList<String>>>();
//			while (kvPairs.size()>1) {
//				int i;
//				for(i = 0;i*2+1 < kvPairs.size(); i++) {
//					newPairs.add(Util.mergeArray(kvPairs.get(i*2), kvPairs.get(i*2+1)));
//				}
//				if(i*2+1 == kvPairs.size()) {
//					newPairs.add(kvPairs.get(i*2));
//				}
//				kvPairs = newPairs;
//				newPairs = new ArrayList<HashMap<String, ArrayList<String>>>();
//			}
//			
//			HashMap<String, ArrayList<String>> values = kvPairs.get(0);
			
//			for(String key : values.keySet()) {
//				reducer.reduce(key, values.get(key), collector);
//			}
			
			
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
		outputFormat output = new outputFormat();
		
		byte[] result = null;
		try {
			result = output.getOutput(collector).toString().getBytes("UTF-8");
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
	
	private String readline(BufferedReader br) throws IOException {
		return br.readLine();
	}

}
