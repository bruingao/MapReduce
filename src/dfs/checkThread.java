package dfs;

import Common.dfsScheduler;
import Common.Util;

public class checkThread implements Runnable{

	private String dnode;
	private int dnodeport;
	private String serviceName;
	
	private OP op;

	private String filename;
	
	private int chunknumber;

	private String[] nodes;

	public enum OP  {WRITE, STATUS, DELETE};
	
	public checkThread(String dn, int dnp, String sname)
	{
		dnode = dn;
		dnodeport = dnp;
		serviceName = sname;
	}
	
	public void setChunknumber(int chunknumber) {
		this.chunknumber = chunknumber;
	}
	
	public void setFilename(String s) {
		filename = s;
	}
	
	public void setOp(OP op) {
		this.op = op;
	}
	
	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}
	
	@Override
	public void run() {
		switch(op) {
			case STATUS:
				checkStatus();
				break;
			case WRITE:
				write();
				break;
			case DELETE:
				delete();
				break;
			default:
				break;
		}
	}
	
	private void checkStatus() {
		boolean status = false;
		try {
			DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
			status = datanode.heartBeat();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			if (e instanceof java.rmi.NotBoundException) {
				System.out.println("Datanode "+dnode+" not startup.");
			}
			else {
				e.printStackTrace();
			}
		} finally {
			dfsScheduler.getStatus().put(dnode, status);
		}
	}
	
	private void write() {
		try {
			DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
			boolean res = datanode.replication(filename + chunknumber, nodes);
			
			if (res) {
				dfsScheduler.replication(filename, chunknumber, dnode);
				/* checkpoint */
				Util.writeObject(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
				Util.writeObject(NameNode.nameNodePath+"nodeToReplicas", dfsScheduler.getNodeToReplicas());
			}
			
		} catch (Exception e) {
			if (e instanceof java.rmi.NotBoundException) {
				System.out.println("Datanode "+dnode+" not startup.");
			}
			else {
				e.printStackTrace();
			}
		} 
	}
	
	private void delete() {
		try {
			DataNodeI datanode = (DataNodeI) NameNode.registry.lookup(dnode+"/"+serviceName);
			datanode.removeFile(filename + chunknumber);
			
			dfsScheduler.removeReplica(filename, chunknumber, dnode);
			/* checkpoint */
			Util.writeObject(NameNode.nameNodePath+"files", dfsScheduler.getFiles());
			Util.writeObject(NameNode.nameNodePath+"nodeToReplicas", dfsScheduler.getNodeToReplicas());

		} catch (Exception e) {
			if (e instanceof java.rmi.NotBoundException) {
				System.out.println("Datanode "+dnode+" not startup.");
			}
			else {
				e.printStackTrace();
			}
		} 
	}

}
