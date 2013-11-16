package mapred;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mapred.JobTracker.JOB_RESULT;

import Common.Pair;
import Common.Util;

/**
 * JobClient is client side program which can be running on
 * a machine outside of the MapReduce framework. It will set
 * configurations of a job according to user's settings,
 * submit the job to JobTracker in the MapReduce framework,
 * check progress, resubmit job upon failures, etc. Users of
 * the MapReduce Programming framework will have to instantiate
 * a JobClient in their main programs and run it with their
 * desired configurations of mapper/reducer classes, input/output
 * formats, and input/output destinations.
 *
 * @author      Rui Zhang
 * @author      Jing Gao
 * @version     1.0, 11/12/2013
 * @since       1.0
 */
public class JobClient {
    
    private static Registry registry;
    
    private static JobConf jobconf;
    
    private static String confPath = "conf/mapred.conf";
        
    /* job tracker's host address */
    private static String jobHostname;
    
    /* job tracker's port number */
    private static Integer jobPort;
    
    /* task tracker's port number */
    private static Integer taskPort;
    
    /* job trakcer's service name */
    private static String jobServiceName;
    
    private static Integer nameRegPort;
    private static Integer dataRegPort;
    private static Integer jobRegPort;
    private static Integer taskRegPort;
    
    /* maximum fail times */
    private static Integer maxFailTimes;
    
    /* job id */
    private static int jid;
    
    /** 
     * set the configurations for the client job
     * 
     * @param conf      configurations
     * @since           1.0
     */  
    private void setConf(JobConf conf) {
        jobconf = conf;
    }
    
    /** 
     * run job with specified configurations
     * 
     * @param conf      configurations
     * @since           1.0
     */
    public void runJob(JobConf conf) throws RemoteException{
        /* read configuration file */
        JobClient jobclient = new JobClient();
        jobclient.setConf(conf);
        
        Util.readConfigurationFile(confPath, jobclient);
        
        /* submit job to job tacker (submitJob) */
        
        JobTrackerI jobtracker = null;
        
        try {
            registry = LocateRegistry.getRegistry(jobHostname, jobRegPort);
            jobtracker = (JobTrackerI)registry.lookup(jobServiceName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception happend when looking for services of JobTracker!");
            System.exit(-1);
        }
        
        String mapperName = conf.getMapperClass().getName().replace('.', '/');
        String reducerName = conf.getReducerClass().getName().replace('.', '/');

        
        Pair mapper = new Pair(conf.getMapperClass().getName(), 
                Util.readFromFile(mapperName+".class"));
        Pair reducer = new Pair(conf.getReducerClass().getName(),
                Util.readFromFile(reducerName+ ".class"));
        String res = jobtracker.submitJob(conf, mapper, reducer);
        
        /* return the job id or some errors may occur */
        if (res.equals("INPUTNOTFOUND")) {
            System.out.println("input file not found!");
            System.exit(-1);
        } 
        
        int failtimes = 0;
        while (res.equals("FAIL")) {
            System.out.println("Job failed!");
            if (failtimes < maxFailTimes) {
                System.out.println("job restarting!");
                res = jobtracker.submitJob(conf, mapper, reducer);
                failtimes++;
                continue;
            } else {
                jobtracker.terminateJob(jid);
                System.out.println("Job terminated!");
                System.exit(-1);
            }
            
        }
        
        jid = Integer.parseInt(res);
        
        /* periodically check the status (checkStatus)
         * if INPROGRESS check the percetage (checkMapper and checkReducer)
         * such as map ?% and reduce ?% until both 100% or return FAIL */
        while (true) {
            JOB_RESULT status = jobtracker.checkStatus(jid);
            if(status == JOB_RESULT.INPROGRESS) {
                double mp = jobtracker.checkMapper(jid);
                double rp = jobtracker.checkReducer(jid);
                System.out.printf("Mapper: %fpercent; Reducer: %fpercent\n", mp*100, rp*100);
            } else if (status == JOB_RESULT.FAIL) {
                System.out.println("job failed!");
                if(failtimes < maxFailTimes) {
                    failtimes++;
                    res = jobtracker.submitJob(conf, mapper, reducer);
                    if(res.equals("FAIL") &&  res.equals("INPUTNOTFOUND"))
                        jid = Integer.parseInt(res);
                    continue;
                } else {
                    jobtracker.terminateJob(jid);
                    System.out.println("job terminated!");
                    break;
                }
            } else if (status == JOB_RESULT.SUCCESS) {
                System.out.printf("Mapper: 100percent; Reducer: 100percent\n");

                System.out.println("job succeed! You can check your output files use the name format: " +
                        "[jobid]-[outputfilename]-part-[partition number]");
                
                System.out.println("Your jobid is "+jid + ", and your outputfile name is "+conf.getOutputfile());
                jobtracker.terminateJob(jid);
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("Exception happened when monitoring!");
            }
        }
        
    }
}
