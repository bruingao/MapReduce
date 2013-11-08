package Common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Scheduler {
	
	/* filename to chunks, chunk number to its datanodes which it resides on */
	private static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> files
		= new ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>();

	/* which files reside on this datanode */
	private static ConcurrentHashMap<String, HashSet<String>> nodeToReplicas
		= new ConcurrentHashMap<String, HashSet<String>>();
	
	

	/* datanodes' status */
	private static ConcurrentHashMap<String, Boolean> status 
		= new ConcurrentHashMap<String, Boolean>();

	/* temp files (need to be veried whether write success, cannot be accessed) */
	private static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> tempFiles
		= new ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>();
	
	
	public static void setFiles(
			ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> files) {
		Scheduler.files = files;
	}

	public static void setNodeToReplicas(
			ConcurrentHashMap<String, HashSet<String>> nodeToReplicas) {
		Scheduler.nodeToReplicas = nodeToReplicas;
	}

	public static void setStatus(ConcurrentHashMap<String, Boolean> status) {
		Scheduler.status = status;
	}

	public static void setTempFiles(
			ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> tempFiles) {
		Scheduler.tempFiles = tempFiles;
	}

	private Scheduler(){}
	
	public static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> getFiles() {
		return files;
	}

	public static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> getTempFiles() {
		return tempFiles;
	}
	
	public static ConcurrentHashMap<String, HashSet<String>> getNodeToReplicas() {
		return nodeToReplicas;
	}
	
	public static ConcurrentHashMap<String, Boolean> getStatus() {
		return status;
	}
	
	/* get the file's information */
	public static HashMap<Integer, HashSet<String>> getFile(String filename) {
		return files.get(filename);
	}
	
	private static boolean checkname(String filename) {
		return files.containsKey(filename) || tempFiles.containsKey(filename); 
	}
	
	/* create new file entry, stored in tempfile table */
	public static HashMap<Integer, HashSet<String>> createFile(String filename, int chunks, int replicas) {
		if(checkname(filename))
			return null;
		
		int num = chunks * replicas;
		String set[] = new String[num];
		int counts[] = new int[num];
		int cnt = 0;
			
		Iterator<Map.Entry<String, HashSet<String>>> it = nodeToReplicas.entrySet().iterator();
		
		/* determine the nodes with minimum data files */
		while(it.hasNext()) {
			Map.Entry<String, HashSet<String>> entry = it.next();
			
			int size = entry.getValue().size();
			
			if(cnt < num) {
				set[cnt] = entry.getKey();
				counts[cnt] = size;
			}
			else {
				int mindex = 0;
				for(int i = 1; i < num; i++) {
					if(counts[i] > counts[mindex]) {
						mindex = i;
					}
				}
				if(size < counts[mindex]) {
					set[mindex] = entry.getKey();
					counts[mindex] = size;
				}
			}
		}
		
		HashMap<Integer, HashSet<String>> res = new HashMap<Integer, HashSet<String>>();
		
		for(int i = 0; i < chunks; i++) {
			HashSet<String> temp = new HashSet<String>();
			for(int j = 0; j < replicas; j++) {
				temp.add(set[i*replicas + j]);
			}
			res.put(i, temp);
		}
		
		tempFiles.put(filename, res);
		
		return res;
		
	}
	
	public static void transferTemp(String filename) {
		
		files.put(filename, tempFiles.remove(filename));
	
	}
	
	public static void deleteTemp(String filename) {
		tempFiles.remove(filename);
	}
	
	
}
