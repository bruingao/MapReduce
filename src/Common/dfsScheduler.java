package Common;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

public final class dfsScheduler {
	
	/* filename to chunks, chunk number to its datanodes which it resides on */
	private static ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> files
		= new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();

	/* which files reside on this datanode */
	private static ConcurrentHashMap<String, HashSet<String>> nodeToReplicas
		= new ConcurrentHashMap<String, HashSet<String>>();
	
	private static ConcurrentHashMap<String, Integer> nodeToFileNum
		= new ConcurrentHashMap<String, Integer>();

	/* datanodes' status */
	private static ConcurrentHashMap<String, Boolean> status 
		= new ConcurrentHashMap<String, Boolean>();

	/* temp files (need to be veried whether write success, cannot be accessed) */
	private static ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tempFiles
		= new ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>>();
	
	
	public static void setFiles(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> files) {
		dfsScheduler.files = files;
	}

	public static void setNodeToReplicas(
			ConcurrentHashMap<String, HashSet<String>> nodeToReplicas) {
		dfsScheduler.nodeToReplicas = nodeToReplicas;
	}

	public static void setStatus(ConcurrentHashMap<String, Boolean> status) {
		dfsScheduler.status = status;
	}

	public static void setTempFiles(
			ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tempFiles) {
		dfsScheduler.tempFiles = tempFiles;
	}

	private dfsScheduler(){}
	
	public static ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFiles() {
		return files;
	}

	public static ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getTempFiles() {
		return tempFiles;
	}
	
	public static ConcurrentHashMap<String, HashSet<String>> getNodeToReplicas() {
		return nodeToReplicas;
	}
	
	public static ConcurrentHashMap<String, Boolean> getStatus() {
		return status;
	}
	
	/* get the file's information */
	public static Hashtable<Integer, HashSet<String>> getFile(String filename) {
		return files.get(filename);
	}
	
	public static boolean checkname(String filename) {
		return files.containsKey(filename) || tempFiles.containsKey(filename); 
	}
	
	/* choose most lightweight nodes */
	public static String[] chooseLight(int num, HashSet<String> nodes) {
		
		String res[] = new String[num];
		int sizes[] = new int[num];
				
		int cnt = 0;
				
		for (String datanode : nodeToReplicas.keySet()) {
//			int size = nodeToReplicas.get(datanode).size();
			int size = nodeToFileNum.get(datanode);
			
			if((nodes != null && nodes.contains(datanode)) || (!status.get(datanode)))
				continue;
			
			if(cnt < num) {
				res[cnt] = datanode;
				sizes[cnt] = size;
				cnt++;
			}
			else {
				int mindex = 0;
				for(int i = 1; i < num; i++) {
					if(sizes[i] > sizes[mindex]) {
						mindex = i;
					}
				}
				if(size < sizes[mindex]) {
					res[mindex] = datanode;
					sizes[mindex] = size;
				}
			}
		}
		
		for (String datanode : res) {
			if(datanode == null)
				break;
			nodeToFileNum.put(datanode, nodeToFileNum.get(datanode)+1);
		}
		
		return res;
	}
	
	public static ConcurrentHashMap<String, Integer> getNodeToFileNum() {
		return nodeToFileNum;
	}

	public static void setNodeToFileNum(
			ConcurrentHashMap<String, Integer> nodeToFileNum) {
		dfsScheduler.nodeToFileNum = nodeToFileNum;
	}

	/* choose most heavy nodes */
	public static String[] chooseHeavy(int num , String[] nodes) {
				
		if(nodes == null)
			nodes = (String[]) nodeToReplicas.keySet().toArray();
		
		if(nodes.length < num){
			return nodes;
		}
		
		String res[] = new String[num];
		int sizes[] = new int[num];
				
		int cnt = 0;
		
		for (String datanode : nodes) {
			int size = nodeToReplicas.get(datanode).size();
			
			if(cnt < num) {
				res[cnt] = datanode;
				sizes[cnt] = size;
			}
			else {
				int mindex = 0;
				for(int i = 1; i < num; i++) {
					if(sizes[i] < sizes[mindex]) {
						mindex = i;
					}
				}
				if(size > sizes[mindex]) {
					res[mindex] = datanode;
					sizes[mindex] = size;
				}
			}
		}
		
		return res;
	}
	
	/* get a file's corresponding information */
	public static Hashtable<Integer, HashSet<String>> openFile(String filename) {
		
		Hashtable<Integer, HashSet<String>> temp = dfsScheduler.getFile(filename);
		
		if(temp == null)
			return null;
		
		Hashtable<Integer, HashSet<String>> res = 
				new Hashtable<Integer, HashSet<String>> (temp);
		
		for (int chunk : temp.keySet()) {
			for (String node :temp.get(chunk)) {
				if(!status.get(node)) {
					res.get(chunk).remove(node);
				}
			}
		}
		
		return res;
	}
	
	
	/* create new file entry, stored in tempfile table */
	public static Hashtable<Integer, HashSet<String>> createFile(String filename, int chunks, int replicas) {
		if(checkname(filename))
			return null;
				
		Hashtable<Integer, HashSet<String>> res = new Hashtable<Integer, HashSet<String>>();
		
		for(int i = 0; i < chunks; i++) {
			String set[] = chooseLight(replicas, null);
			HashSet<String> temp = new HashSet<String>();
			for(int j = 0; j < replicas; j++) {
				if(set[j] != null)
					temp.add(set[j]);
				else 
					return new Hashtable<Integer, HashSet<String>>();
			}
			res.put(i, temp);
		}
		
		tempFiles.put(filename, res);
				
		return res;
	}
	
	public static void replication (String filename, int chunknumber, String node) {
		files.get(filename).get(chunknumber).add(node);
		
		nodeToReplicas.get(node).add(filename+chunknumber);
		nodeToFileNum.put(node, nodeToFileNum.get(node)+1);
	}
	
	public static void removeReplica (String filename, int chunknumber, String node) {
		files.get(filename).get(chunknumber).remove(node);
		
		nodeToReplicas.get(node).remove(filename + chunknumber);
		nodeToFileNum.put(node, nodeToFileNum.get(node)-1);

	}
	
	public static void transferTemp(String filename) {
		
		Hashtable<Integer,HashSet<String>> res = tempFiles.remove(filename);
		
		files.put(filename, res);
		
		for(Integer chunk : res.keySet()) {
			for (String node : res.get(chunk)) {
				nodeToReplicas.get(node).add(filename+chunk);
			}
		}
		
	}
	
	public static void deleteTemp(String filename) {
		for(int chunk : tempFiles.get(filename).keySet()) {
			for (String node : tempFiles.get(filename).get(chunk)) {
				nodeToFileNum.put(node, nodeToFileNum.get(node)-1);
			}
		}
		
		tempFiles.remove(filename);
	}
	
	public static void removeFile(String filename) {
		for(Integer c : files.get(filename).keySet()) {
			for (String node : files.get(filename).get(c)) {
				nodeToReplicas.get(node).remove(filename+c);
			}
		}
		
		files.remove(filename);
	}
	
	
}
