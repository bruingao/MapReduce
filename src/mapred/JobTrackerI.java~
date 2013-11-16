package mapred;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Common.Pair;

import mapred.JobTracker.JOB_RESULT;
import mapred.JobTracker.NOTIFY_RESULT;

public interface JobTrackerI extends Remote{
	public String submitJob(JobConf conf, Pair mapper, Pair reducer) throws RemoteException;
	public JOB_RESULT checkStatus(Integer jobid) throws RemoteException;
	public double checkMapper(Integer jobid) throws RemoteException;
	public double checkReducer(Integer jobid) throws RemoteException;
	
	public Pair readMapper(Integer jid) throws RemoteException;
	public Pair readReducer(Integer jid) throws RemoteException;
	
	public void notifyMapResult(NOTIFY_RESULT res, int jid, String tnode) throws RemoteException;
	
	public void notifyReduceResult(NOTIFY_RESULT res, int jid, String tnode, int partition) throws RemoteException;
	
	public void terminateJob(int jid) throws RemoteException;
}
