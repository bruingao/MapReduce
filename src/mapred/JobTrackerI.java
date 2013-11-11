package mapred;

import java.rmi.Remote;
import java.rmi.RemoteException;

import mapred.JobTacker.JOB_RESULT;

public interface JobTrackerI extends Remote{
	public JOB_RESULT submitJob(JobConf conf) throws RemoteException;
	public JOB_RESULT checkStatus(Integer jobid) throws RemoteException;
	public double checkMapper(Integer jobid) throws RemoteException;
	public double checkReducer(Integer jobid) throws RemoteException;
}
