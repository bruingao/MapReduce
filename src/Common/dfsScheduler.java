package Common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * dfsScheduler is the scheduler/helper class in DFS. It keeps records of
 * information about files, temporary files (files before write verification),
 * DataNodes and their status. It is capable of arranging locations of file
 * chunk replicas according to the load of each DataNodes.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public final class dfsScheduler {
	
	/* hashmap of filename to chunks, chunk number to residing DataNodes */
	private static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> files
		= new ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>();

	/* hashmap of DataNode to file chunks on it */
	private static ConcurrentHashMap<String, HashSet<String>> nodeToReplicas
		= new ConcurrentHashMap<String, HashSet<String>>();
	
	/* hashmap of DataNodes and status */
	private static ConcurrentHashMap<String, Boolean> status 
		= new ConcurrentHashMap<String, Boolean>();

	/* hashmap of temp files (need to be verified whether write succeeded, cannot be accessed) */
	private static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> tempFiles
		= new ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>>();
	
	/** 
     * constructor of dfsScheduler class
     * 
     * @since           1.0
     */
	private dfsScheduler(){}
		
	/** 
     * set records of files as the hashmap of file name, hashmap of chunk number and DataNodes
     * 
     * @param files     the hashmap of file name, hashmap of chunk number and DataNodes
     * @since           1.0
     */
	public static void setFiles(
			ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> files) {
		dfsScheduler.files = files;
	}

    /** 
     * set records of DataNodes as the hashmap of DataNodes and the file replicas on them
     * 
     * @param nodeToReplicas    the hashmap of DataNodes and the file replicas on them
     * @since                   1.0
     */
	public static void setNodeToReplicas(
			ConcurrentHashMap<String, HashSet<String>> nodeToReplicas) {
		dfsScheduler.nodeToReplicas = nodeToReplicas;
	}
    
    /** 
     * set records of status as the hashmap of DataNodes and their status
     * 
     * @param status    the hashmap of DataNodes and their status
     * @since           1.0
     */
	public static void setStatus(ConcurrentHashMap<String, Boolean> status) {
		dfsScheduler.status = status;
	}
    
    /** 
     * set records of temp files as the hashmap of file name, hashmap of chunk number and DataNodes
     * 
     * @param files     the hashmap of temp file name, hashmap of chunk number and DataNodes
     * @since           1.0
     */
	public static void setTempFiles(
			ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> tempFiles) {
		dfsScheduler.tempFiles = tempFiles;
	}
	
	/** 
     * get information about files in the DFS
     * 
     * @return      the hashmap of file name, hashmap of chunk number and residing DataNodes
     * @since       1.0
     */
	public static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> getFiles() {
		return files;
	}
    
    /** 
     * get information about temp files
     * 
     * @return      the hashmap of temp file name, hashmap of chunk number and residing DataNodes
     * @since       1.0
     */
	public static ConcurrentHashMap<String, HashMap<Integer, HashSet<String>>> getTempFiles() {
		return tempFiles;
	}
	
	/** 
     * get information about DataNodes and the replicas on them
     * 
     * @return      the hashmap of DataNodes and file replicas on them
     * @since       1.0
     */
	public static ConcurrentHashMap<String, HashSet<String>> getNodeToReplicas() {
		return nodeToReplicas;
	}
	
	/** 
     * get information about the status of DataNodes
     * 
     * @return      the hashmap of temp file name, hashmap of chunk number and residing DataNodes
     * @since       1.0
     */
	public static ConcurrentHashMap<String, Boolean> getStatus() {
		return status;
	}
	
    /** 
     * get information about a file in DFS, including its chunks their residing DataNodes
     * 
     * @return      the hashmap of chunk numbers and corresponding DataNodes
     * @since       1.0
     */
	public static HashMap<Integer, HashSet<String>> getFile(String filename) {
		return files.get(filename);
	}
	
	/** 
     * check if a specified file exists in DFS
     * 
     * @return      true if the file is in DFS; false otherwise
     * @since       1.0
     */
	public static boolean checkname(String filename) {
		return files.containsKey(filename) || tempFiles.containsKey(filename); 
	}
	
	/** 
     * choose a number of DataNodes with least loads
     * 
     * @param num       the number of DataNodes needed
     * @param nodes     the candidate DataNodes
     * @return          array of chosen DataNodes hostnames
     * @since           1.0
     */
	public static String[] chooseLight(int num, HashSet<String> nodes) {
		String res[] = new String[num];
		int sizes[] = new int[num];
		int cnt = 0;
		for (String datanode : nodeToReplicas.keySet()) {
			int size = nodeToReplicas.get(datanode).size();
			if(nodes != null && nodes.contains(datanode) && (!status.get(datanode)))
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
		return res;
	}
	
	/** 
     * choose a number of DataNodes with most loads
     * 
     * @param num       the number of DataNodes needed
     * @param nodes     the candidate DataNodes
     * @return          array of chosen DataNodes hostnames
     * @since           1.0
     */
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
	
	/** 
     * get the locations of file chunks when opening a file
     * 
     * @param filename      the name of the file to open
     * @return              the hashmap of chunks and their residing DataNodes
     * @since               1.0
     */
	public static HashMap<Integer, HashSet<String>> openFile(String filename) {
		HashMap<Integer, HashSet<String>> res = 
				(HashMap<Integer, HashSet<String>>) dfsScheduler.getFile(filename).clone();
		for (int chunk : res.keySet()) {
			for (String node :res.get(chunk)) {
				if(!status.get(node)) {
					res.get(chunk).remove(node);
				}
			}
		}
		
		return res;
	}
	
	
	/** 
     * arrange the distribution of file chunk replicas when creating a file in the DFS
     * 
     * @param filename      the name of the file to be created
     * @param chunks        the chunk number of the file
     * @param replicas      the number of file chunk replicas
     * @return              the hashmap of chunk numbers and residing DataNodes
     * @since               1.0
     */
	public static HashMap<Integer, HashSet<String>> createFile(String filename, int chunks, int replicas) {
		if(checkname(filename))
			return null;
		HashMap<Integer, HashSet<String>> res = new HashMap<Integer, HashSet<String>>();
		for(int i = 0; i < chunks; i++) {
			String set[] = chooseLight(replicas, null);
			HashSet<String> temp = new HashSet<String>();
			for(int j = 0; j < replicas; j++) {
				if(set[j] != null)
					temp.add(set[j]);
				else 
					return new HashMap<Integer, HashSet<String>>();
			}
			res.put(i, temp);
		}
		tempFiles.put(filename, res);		
		return res;
	}
	
	/** 
     * update information after replication of a file chunk
     * 
     * @param filename      the name of the replicated file
     * @param chunknumber   the chunk number of the file
     * @param node          the DataNode on which the replication happened 
     * @since               1.0
     */
	public static void replication (String filename, int chunknumber, String node) {
		files.get(filename).get(chunknumber).add(node);
		nodeToReplicas.get(node).add(filename+chunknumber);
	}
	
	/** 
     * update information after removing replica of a file chunk
     * 
     * @param filename      the name of the file
     * @param chunknumber   the chunk number of the file
     * @param node          the DataNode on which the removal happened 
     * @since               1.0
     */
	public static void removeReplica (String filename, int chunknumber, String node) {
		files.get(filename).get(chunknumber).remove(node);
		nodeToReplicas.get(node).remove(filename + chunknumber);
	}
	
	/** 
     * update information after write success (temp file is verified)
     * 
     * @param filename      the name of the verified file
     * @since               1.0
     */
	public static void transferTemp(String filename) {
		HashMap<Integer,HashSet<String>> res = tempFiles.remove(filename);
		files.put(filename, res);
		for(Integer chunk : res.keySet()) {
			for (String node : res.get(chunk)) {
				nodeToReplicas.get(node).add(filename+chunk);
			}
		}
	}
	
	/** 
     * delete a temp file
     * 
     * @param filename      the name of the temp file
     * @since               1.0
     */
	public static void deleteTemp(String filename) {
		tempFiles.remove(filename);
	}
    
    /** 
     * update information after removing a file
     * 
     * @param filename      the name of the removed file
     * @since               1.0
     */	
	public static void removeFile(String filename) {
		for(Integer c : files.get(filename).keySet()) {
			for (String node : files.get(filename).get(c)) {
				nodeToReplicas.get(node).remove(filename+c);
			}
		}
    	files.remove(filename);
	}
	
	
}
