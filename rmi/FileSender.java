package rmi;

import java.rmi.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;


public class FileSender{

	FileReceiver receiver;
	
	public FileSender( String hostname ) throws Exception {

		Registry registry = LocateRegistry.getRegistry(hostname);
   	  receiver = 
    	(FileReceiver)registry.lookup( "//" + hostname + "/receiver" );
    }
    
    
 	public static void main( String args[] ) throws Exception {
		String usage = "usage: java rmi.FileSender <filepath> <remotehost>";
		try{
			System.setSecurityManager( new RMISecurityManager() );

			FileSender sender = new FileSender( args[1] );
			String filename = args[0];
  		    System.out.println( "Sending: " + filename );
   		    FilePacket packet = new FilePacket( filename );
   		    packet.readIn();
   		    sender.receiver.receiveFile( packet );
			
		}catch( Exception e ){
			e.printStackTrace();
			System.out.println( usage );
		}
	}
				
}
