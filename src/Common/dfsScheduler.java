package Common;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;


/* dfsScheduler works on NameNode and it is a helper to NameNode.
 * An important functionality of dfsScheduler is scheduling, i. e.,
 * arranging the distribution of file chunks according to the storage
 * loads of DataNodes and meeting the requirement of replication
 * factor. Other than scheduling, dfsScheduler keeps all information
 * about the distributed file system in several hash tables, which
 * include files and the locations of their chunks, DataNode host names
 * and the file chunks stored on each of them, storage load of each
 * DataNode, status of each DataNode, information about temporary files
 * (generated before verification of write success). NameNode updates
 * the above information whenever changes happen in the distributed
 * file system, and it looks up the required information and responding
 * to other components or to the queries from client side.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
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
    
    /**
     * set info about DFS files
     *
     * @param files     file chunks and their locations
     * @since           1.0
     */
    public static void setFiles(
            ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> files) {
        dfsScheduler.files = files;
    }

    /**
     * set info about DFS nodes and file replicas on them
     *
     * @param nodeToReplicas    nodes and the replicas on them
     * @since                   1.0
     */
    public static void setNodeToReplicas(
            ConcurrentHashMap<String, HashSet<String>> nodeToReplicas) {
        dfsScheduler.nodeToReplicas = nodeToReplicas;
    }

    /**
     * set info about node status
     *
     * @param status    nodes and their status
     * @since           1.0
     */
    public static void setStatus(ConcurrentHashMap<String, Boolean> status) {
        dfsScheduler.status = status;
    }

    /**
     * set info about DFS temp files
     *
     * @param tempFiles     file chunks and their locations
     * @since           1.0
     */
    public static void setTempFiles(
            ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> tempFiles) {
        dfsScheduler.tempFiles = tempFiles;
    }

    /**
     * constructor of dfsScheduler class
     *
     * @since           1.0
     */
    private dfsScheduler(){}
    
    /**
     * get info about DFS files
     *
     * @return          file chunks and their locations
     * @since           1.0
     */
    public static ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getFiles() {
        return files;
    }

    /**
     * get info about DFS temp files
     *
     * @return          temp file chunks and their locations
     * @since           1.0
     */
    public static ConcurrentHashMap<String, Hashtable<Integer, HashSet<String>>> getTempFiles() {
        return tempFiles;
    }
    
    /**
     * get info about DFS nodes and replicas on them
     *
     * @return          nodes and their replicas
     * @since           1.0
     */
    public static ConcurrentHashMap<String, HashSet<String>> getNodeToReplicas() {
        return nodeToReplicas;
    }
    
    /**
     * get info about DFS nodes status
     *
     * @return          nodes and their status
     * @since           1.0
     */
    public static ConcurrentHashMap<String, Boolean> getStatus() {
        return status;
    }
    
    /* get the file's information */
    public static Hashtable<Integer, HashSet<String>> getFile(String filename) {
        return files.get(filename);
    }
    
    /**
     * check if a file exists in DFS
     *
     * @param filename  destination file name
     * @return          true if file exists in DFS; false otherwise
     * @since           1.0
     */
    public static boolean checkname(String filename) {
        return files.containsKey(filename) || tempFiles.containsKey(filename); 
    }
    
    /**
     * choose most lightweight nodes
     *
     * @param num       destination number of nodes
     * @param nodes     candidate nodes
     * @return          array of chosen nods
     * @since           1.0
     */
    public static String[] chooseLight(int num, HashSet<String> nodes) {
        
        String res[] = new String[num];
        int sizes[] = new int[num];
                
        int cnt = 0;
                
        for (String datanode : nodeToReplicas.keySet()) {
//            int size = nodeToReplicas.get(datanode).size();
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
    
    /**
     * get number of file chunks on each node
     *
     * @return          nodes and their storage loads
     * @since           1.0
     */

    public static ConcurrentHashMap<String, Integer> getNodeToFileNum() {
        return nodeToFileNum;
    }

    /**
     * set number of file chunks on each node
     *
     * @param nodeToFileNum     the records to set
     * @since                   1.0
     */
    public static void setNodeToFileNum(
            ConcurrentHashMap<String, Integer> nodeToFileNum) {
        dfsScheduler.nodeToFileNum = nodeToFileNum;
    }

    /**
     * choose most heavyweight nodes
     *
     * @param num       destination number of nodes
     * @param nodes     candidate nodes
     * @return          array of chosen nods
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
     * get chunk locations of an opening file
     *
     * @param filename  the name of file to open
     * @return          chunks and locations
     * @since           1.0
     */
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
    
    
    /**
     * get chunk locations of a file to be created
     *
     * @param filename  the name of file to open
     * @param chunks    desired chunk number
     * @param replicas  replication factor
     * @return          chunks and locations
     * @since           1.0
     */
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
    
    /**
     * replicate file chunk from another node
     *
     * @param filename      the name of file
     * @param chunknumber   chunk number
     * @param node          source node
     * @since               1.0
     */
    public static void replication (String filename, int chunknumber, String node) {
        files.get(filename).get(chunknumber).add(node);
        
        nodeToReplicas.get(node).add(filename+chunknumber);
        nodeToFileNum.put(node, nodeToFileNum.get(node)+1);
    }
    
    /**
     * remove file replica from a node
     *
     * @param filename      the name of file
     * @param chunknumber   the chunk number
     * @param node          the destination node
     * @since           1.0
     */
    public static void removeReplica (String filename, int chunknumber, String node) {
        files.get(filename).get(chunknumber).remove(node);
        
        nodeToReplicas.get(node).remove(filename + chunknumber);
        nodeToFileNum.put(node, nodeToFileNum.get(node)-1);

    }
    
    /**
     * transfer temp file to verified file
     *
     * @param filename  the name of file
     * @since           1.0
     */
    public static void transferTemp(String filename) {
        
        Hashtable<Integer,HashSet<String>> res = tempFiles.remove(filename);
        
        files.put(filename, res);
        
        for(Integer chunk : res.keySet()) {
            for (String node : res.get(chunk)) {
                nodeToReplicas.get(node).add(filename+chunk);
            }
        }
        
    }
    
    /**
     * delete temp file
     *
     * @param filename  the name of file
     * @since           1.0
     */

    public static void deleteTemp(String filename) {
        for(int chunk : tempFiles.get(filename).keySet()) {
            for (String node : tempFiles.get(filename).get(chunk)) {
                nodeToFileNum.put(node, nodeToFileNum.get(node)-1);
            }
        }
        
        tempFiles.remove(filename);
    }
    
    /**
     * remove a file in DFS
     *
     * @param filename  the name of file
     * @since           1.0
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
