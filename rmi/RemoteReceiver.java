package rmi;

import java.rmi.*;
import java.rmi.server.*;
import java.io.*;
import java.util.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class RemoteReceiver extends UnicastRemoteObject implements FileReceiver {

  private String directory;

  public RemoteReceiver() throws RemoteException {
  	super();
  }

  public void setDirectory( String directory ) throws RemoteException{
  	this.directory = directory;
  }


  public void receiveFile( FilePacket packet ) throws RemoteException {
   	try{
   	    String filename=packet.getName();
   	    String shortFilename=filename.substring(filename.lastIndexOf("/")+1);
       	packet.writeTo( new FileOutputStream( this.directory+shortFilename + ".snt" ) );
    }catch( IOException e ){
      e.printStackTrace();
    }
  }

public void proxyRebind(FileReceiver receiver) throws RemoteException{
}

  public static void main( String args[] ) {
  	String usage = "usage: java rmi.RemoteReceiver <directory> <hostname>";
  	try{
  		
  		if( args.length != 2 ){
  			System.out.println( usage );
  			System.exit( 0 );
  		}
  		System.setSecurityManager( new RMISecurityManager() );
    	RemoteReceiver remote = new RemoteReceiver();
    	remote.setDirectory( args[0] );
    	System.out.println( "Binding object" );
    	
    	Registry registry = LocateRegistry.getRegistry(args[1]);
    	registry.rebind( "//" + args[1] + "/receiver", remote );
    	System.out.println( "Object is bound" );
    }catch( Exception e ){
    	e.printStackTrace();
    	System.out.println( usage );
    }
  }

}
