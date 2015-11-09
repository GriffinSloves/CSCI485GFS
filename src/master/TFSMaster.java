package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;

public class TFSMaster{
	
	public final static String ClientMasterConfigFile = "MasterConfig.txt";
	public static HashSet<String> namespace; //maps directory paths to IP address of chunk servers
	public HashMap<String, String[]> filesToChunkHandles; // maps which chunks constitute a file
	public HashMap<String, String> chunkHandlesToServers; //maps chunk handles to locations of their replicas(CS IP addresses)
	
	public TFSMaster()
	{
		namespace.add("/"); //to create the overall source of the namespace
		
		ServerSocket ss = null;
		try{
			ss = new ServerSocket(6789);
			while (true)
			{
				Socket s = ss.accept();
				System.out.println("Client connected to master");
				ServerThread st = new ServerThread(s, this);
				st.start();
			}
		}
		catch (IOException ioe) {ioe.printStackTrace();}
	}

	
	class ServerThread extends Thread
	{
		Socket s; TFSMaster master;
		ObjectInputStream ois; ObjectOutputStream oos;
		
		public ServerThread(Socket s, TFSMaster master)
		{
			try {
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
				this.master = master;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void run()
		{
			while (true)//process requests
			{
				try {
					
					String command = (String) ois.readObject();
					if (command.equals("CreateDir"))
					{
						createDir();
					}
					if (command.equals("DeleteDir"))
					{
						deleteDir();
					}
					if (command.equals("RenameDir"))
					{
						renameDir();
					}
					if (command.equals("openFile"))
					{
						openFile();
					}
					if (command.equals("CreateDir"))
					{
						closeFile();
					}
					
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}
		public void createDir() throws IOException, ClassNotFoundException
		{
			//check if the src doesn't exist
			String srcDirectory = (String) ois.readObject();
			boolean checkSrcExists = namespace.contains(srcDirectory);//if this returns null, there is no match
			if (!checkSrcExists) {
				oos.writeObject("does_not_exist");
				oos.flush();
				return;
			}
			else{
				oos.writeObject(""); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			
			//check if directory exists
			String dirname = (String) ois.readObject();
			boolean checkDirExists = namespace.contains(srcDirectory+"/"+dirname);//if this returns null, there is no match
			if (checkDirExists) {
				oos.writeObject("dir_exists");
				oos.flush();
				return;
			}
			else{
				oos.writeObject(""); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//create the directory in the namespace
			namespace.add(srcDirectory+"/"+dirname);
			
			//send confirmation
			oos.writeObject("success");
			oos.flush();
			
		}
		public void deleteDir()
		{
			
		}
		public void renameDir()
		{
			
		}
		public void listDir()
		{
			
		}
		public void openFile()
		{
			try {
				//read which file wants to be opened
				String filePath = (String) ois.readObject();
				
				//use lookup table to get handles of all chunks of that file
				String[] chunksOfFile = filesToChunkHandles.get(filePath);
				
				//send back to ClientFS to be loaded into the Filehandle object
				oos.writeObject(chunksOfFile);
				oos.flush();
				
				//for each chunk, get its location -- IP address of chunkserver
				HashMap<String, String> ChunkLocations = new HashMap<String, String>();
				for (int i = 0; i < chunksOfFile.length; i++)
				{
					String location = chunkHandlesToServers.get(chunksOfFile[i]);
					ChunkLocations.put(chunksOfFile[i], location);
				}
				
				//send that array back to ClientFS to load into FileHandle object
				oos.writeObject(ChunkLocations);
				oos.flush();
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void closeFile()
		{
			
		}
	}



}
