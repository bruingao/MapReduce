package rmi;

import java.rmi.*;
import java.awt.*;
import java.awt.event.*;

/**
 * A class that will upload a file to the remote server
 **/
public class FileSender implements ActionListener {

	// my frame
	private Frame frame;
	// the remote receiver
	FileReceiver receiver;
	
	public FileSender( String hostname ) throws Exception {
		// get a handle to the remote object
   	receiver = 
    	(FileReceiver)Naming.lookup( "//" + hostname + "/receiver" );
        
   	// make my gui
   	frame = new Frame( "File Sender" );
   	frame.setBounds( 100, 100, 80, 80 );
   	Button b = new Button( "Send File" );
   	b.addActionListener( this );
   	frame.add( "Center", b );
   	frame.setVisible( true );
  }
  
  public void actionPerformed( ActionEvent event ){
  	try{
  		// we are only interested in the send button
  		FileDialog dialog = new FileDialog( frame, "Upload a file",
  																		FileDialog.LOAD );
  		// should block until choice is made
  		dialog.setVisible( true );
  		// choice was made
  		String filename = dialog.getDirectory() + dialog.getFile();
  		System.out.println( "Sending: " + filename );
   		FilePacket packet = new FilePacket( filename );
   		packet.readIn();
   		receiver.receiveFile( packet );
   	}catch( Exception e ){
   		e.printStackTrace();
   	}
	}
    
    
 	public static void main( String args[] ) throws Exception {
		String usage = "Usage: java rmi.FileSender <remotehost>";
		try{
			System.setSecurityManager( new RMISecurityManager() );
			// make an object of my type
			FileSender sender = new FileSender( args[0] );
		}catch( Exception e ){
			e.printStackTrace();
			System.out.println( usage );
		}
	}
				
}
