package master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;



import com.chunkserver.ChunkServer;
import com.client.FileHandle;

public class TFSMaster{
	
	public final static String MasterConfigFile = "MasterConfig.txt";
	public static String currentLogFile;
	public static Vector<String> filesThatHaveBeenDeleted;
	
	public static HashSet<String> namespace; //maps directory paths to IP address of chunk servers
	public HashMap<String, Vector<String>> filesToChunkHandles; // maps which chunks constitute a file
	public HashMap<String, Vector<String>> chunkHandlesToServers; //maps chunk handles to locations of their replicas(CS IP addresses)
	
	public static File nameSpaceFile;
	public static File filesToChunkHandlesFile;
	public static File chunkHandlesToServersFile;
	
	public TFSMaster()
	{
		
		//read all metadata from files on startup
		readMetaData();
		
		ServerSocket ss = null;
		try{
			ss = new ServerSocket(0);//find open socket
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
	
	public void readNameSpace()
	{
		FileReader fr;
		BufferedReader br;
		
		//read namespace data
		this.nameSpaceFile = new File("namespace.txt");
		try {
			fr = new FileReader(nameSpaceFile);
			br = new BufferedReader(fr);
			
			namespace.add("/"); //to create the overall source of the namespace
			while(br.readLine() != null)
			{
				String temp = br.readLine();//read each entry from file
				namespace.add(temp);//add each entry
			}	
		} catch (FileNotFoundException e) {
			System.out.print("FNFE while reading namespace info");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.print("IOE while reading namespace info");
			e.printStackTrace();
		}
	}
	public void readFilesToChunks()
	{
		FileReader fr;
		BufferedReader br;
		//read mapping of files to chunkHandles
		//each entry is in the format full/file/path.ext:chunkHandle1:chunkHandle2:chunkhandleN
		this.filesToChunkHandlesFile = new File("filesToChunkHandles.txt");
		try{
				fr = new FileReader(filesToChunkHandlesFile);
				br = new BufferedReader(fr);
					
				while(br.readLine() != null)
				{
					StringTokenizer str = new StringTokenizer(br.readLine(),":");//read each entry from file
					String fileName = str.nextToken();//the first token should be the file name;
					Vector<String> chunksInFile = new Vector<String>();//to populate with the chunks
					while (str.hasMoreTokens())
					{
						String chunkHandle = str.nextToken();
						chunksInFile.add(chunkHandle);
					}
					//add each entry to the HashMap
					this.filesToChunkHandles.put(fileName, chunksInFile);
				}
		}
		catch (FileNotFoundException e) {
			System.out.print("FNFE while reading filesToChunkHandles");
			e.printStackTrace();	
		}
		catch (IOException e) {
			System.out.print("IOE while reading filesToChunkhandles");
			e.printStackTrace();
		}	
	  }	
	public void readChunksToLocations()
	{
		
		FileReader fr;
		BufferedReader br;
		//read the mapping of chunkHandles to their host Servers
		//will be in the format ChunkHandle:ServerIP,Port,ServerIP2,Port2
		this.chunkHandlesToServersFile=new File("chunkHandlesToServers.txt");
		try{
			
			fr = new FileReader(chunkHandlesToServersFile);
			br = new BufferedReader(fr);
			
			while (br.readLine() != null)
			{
				StringTokenizer str = new StringTokenizer(br.readLine(),":");
				//get the chunkhandle
				String chunkHandle = str.nextToken();
				
				//each chunkHandle is mapped to a vector of strings representing the IP and port of the CS location
				Vector<String> IPPortInfo = new Vector<String>();
				
				//the next token should be a string in this format: ServerIP,Port,Server2,Port2,...ServerIPN,PortN
				//this time separate by comma
				StringTokenizer str2 = new StringTokenizer(str.nextToken(),",");
				while(str2.hasMoreTokens())
				{
					String IPAddressOfChunkServer = str.nextToken();
					String portOfChunkServer = str.nextToken();
					//add the info to the vector in this format: Server,Port
					String toAdd = IPAddressOfChunkServer +","+portOfChunkServer;
					
					IPPortInfo.addElement(IPAddressOfChunkServer);//add the info to the vector to be mapped to the chunkHandle
				}
			}
		}catch (FileNotFoundException e) {
			System.out.print("FNFE while reading chunkHandlesToServers");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.print("IOE while reading chunkHandlesToSevers");
			e.printStackTrace();
		}
	}
	public void readMetaData()
	{
		readNameSpace();
		readFilesToChunks();
		readChunksToLocations();
	}
	public void applyLog()
	{
		if (currentLogFile == null)//if the master is starting up fresh
		{
			File logFile = new File("logfile-1.txt");//create a new logfile
			currentLogFile ="logfile-1.txt";//set the currentLogFile
		}
		try {
			FileReader fr = new FileReader(currentLogFile);
			BufferedReader br = new BufferedReader(fr);
				
			String filename = br.readLine();
				
			//read through logfile and apply operations
			//should be in format: create:srcDirectoryName:directoryToCreateName
			while (br.readLine()!= null)
			{
				String logLine = br.readLine();//read each line of the log
				StringTokenizer str = new StringTokenizer(logLine);
				String command = str.nextToken();//the first token is the command
				
				if (command.equals("createDir"))
				{
					createFromLog(str);
				}
				if (command.equals("rename"))
				{
					
				}
				if (command.equals("delete"))
				{
					deleteFromLog(str);
				}
				if (command.equals("createFile"))
				{
					
				}
				if (command.equals("deleteFile"))
				{
						
				}
			}		
		} catch (FileNotFoundException e) {
			System.out.println("FNFE: Error reading MasterConfig to find current log");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IOE: Error reading MasterConfig to apply current log");
			e.printStackTrace();
		}
			
		
	}
	
	public void createFromLog(StringTokenizer str)
	{
		String src = str.nextToken();
		String directoryToCreateName = str.nextToken();
		
		//check if the src doesn't exist
		boolean checkSrcExists = namespace.contains(src);//if this returns null, there is no match
		if (!checkSrcExists) {
			System.out.println("Source directory: "+ src +" does not exist.");
			return;
		}
		
		//check if directory exists
		boolean checkDirExists = namespace.contains(src+"/"+directoryToCreateName);//if this returns null, there is no match
		if (checkDirExists) {
			System.out.println("Source directory: "+ src+"/"+directoryToCreateName +" already exists.");
			return;
		}
		
		//create the directory in the namespace
		namespace.add(src+"/"+directoryToCreateName);
	}
	public void deleteFromLog(StringTokenizer str)
	{
		String src = str.nextToken();
		String directoryToDelete = str.nextToken();
		//check if the src doesn't exist
		boolean checkSrcExists = namespace.contains(src);//if this returns null, there is no match
		if (!checkSrcExists) {
			System.out.println("Source directory: "+ src +" does not exist.");
			return;
		}
		
		//check if directory exists
		boolean checkDirExists = namespace.contains(src+"/"+directoryToDelete);//if this returns null, there is no match
		if (!checkDirExists) {
			System.out.println("Directory to be deleted doesn't exist");
			return;
		}
		
		
		//iterate through namespace and find all matches where the src/destinationToDelete is a substring
		//this will capture all files/directories within the directory to be deleted
		Iterator it = (Iterator) namespace.iterator();
		while (it.hasNext())
		{
			//check if src/dest is a substring
			String toCheck = (String) it.next();
			if (toCheck.startsWith(src+"/"+directoryToDelete))
			{
				//if its a match add to list of deleted (will be sent to ChunkServers via heartbeat message)
				//then delete it from the namespace
				filesThatHaveBeenDeleted.add(toCheck);
				namespace.remove(toCheck);
			}
		}
	}
	public void renameFromLog(){}
	public void deleteFilefromLog(){}
	public void createFileFromLog(){}
	
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
			
			//append this create operation to the logfile
			if(master.currentLogFile == null) System.out.println("Cannot append to log, current log == null");
			FileOutputStream fos = new FileOutputStream(master.currentLogFile);
			PrintWriter pw = new PrintWriter(fos); 
			pw.println("createDir:"+srcDirectory+"/"+dirname);//create log record of create operation
			pw.close();
			
			//create the directory in the namespace
			namespace.add(srcDirectory+"/"+dirname);
			
			//send confirmation
			oos.writeObject("success");
			oos.flush();
			
		}
		public void deleteDir()
		{
			try{
			
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
				if (!checkDirExists) {
					oos.writeObject("dest_dir_does_not_exist");
					oos.flush();
					return;
				}
				else{
					oos.writeObject(""); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
				
				//iterate through namespace and find all matches where the src/destinationToDelete is a substring
				//this will capture all files/directories within the directory to be deleted
				
				
				Iterator it = (Iterator) namespace.iterator();
				while (it.hasNext())
				{
					//check if src/dest is a substring
					String toCheck = (String) it.next();
					if (toCheck.startsWith(srcDirectory+"/"+dirname))
					{
						//if its a match add to list of deleted (will be sent to ChunkServers via heartbeat message)
						//then delete it from the namespace
						filesThatHaveBeenDeleted.add(toCheck);
						
						//log the delete operation
						//append this create operation to the logfile
						if(master.currentLogFile == null) System.out.println("Cannot append to log, current log == null");
						FileOutputStream fos = new FileOutputStream(master.currentLogFile);
						PrintWriter pw = new PrintWriter(fos); 
						pw.println("deleteDir:"+srcDirectory+"/"+dirname);//create log record of create operation
						pw.close();
						
						//remove from namespace
						namespace.remove(toCheck);
					}
				}
			}catch (IOException IOE){
				IOE.printStackTrace();
			}catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
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
				Vector<String> chunksOfFile = filesToChunkHandles.get(filePath);
				
				//send back to ClientFS to be loaded into the Filehandle object
				oos.writeObject(chunksOfFile);
				oos.flush();
				
				//for each chunk, get its location -- IP address of chunkserver
				//maps chunkHandles to all the locations of its replicas
				HashMap<String, Vector<String>> ChunkLocations = new HashMap<String, Vector<String>>();
				for (int i = 0; i < chunksOfFile.size(); i++)
				{
					Vector<String> location = chunkHandlesToServers.get(chunksOfFile.elementAt(i));
					ChunkLocations.put(chunksOfFile.elementAt(i), location);
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
