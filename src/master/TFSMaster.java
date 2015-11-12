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
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.Vector;



import com.chunkserver.ChunkServer;
import com.chunkserver.Lease;
import com.client.FileHandle;

public class TFSMaster{
	
	public final static String MasterConfigFile = "MasterConfig.txt";
	public static String currentLogFile = "log1.txt";
	public static Vector<String> filesThatHaveBeenDeleted;
	
	public static HashSet<String> namespace; //maps directory paths to IP address of chunk servers
	public LinkedHashMap<String, Vector<String>> filesToChunkHandles; // maps which chunks constitute a file
	public HashMap<String, Vector<Location>> chunkHandlesToServers; //maps chunk handles to locations of their replicas(CS IP addresses)
	public HashMap<String, Lease> ChunkLeaseMap;
	public HashMap<Lease, String> LeaseServerMap;
	
	
	public static final String nameSpaceFile = "namespace.txt";
	public static final String filesToChunkHandlesFile = "filesToChunkHandles.txt";
	public static final String chunkHandlesToServersFile = "chunkHandlesToServers.txt";
	
	public TFSMaster()
	{
		namespace = new HashSet<String>();
		filesToChunkHandles = new LinkedHashMap<String, Vector<String>>();
		chunkHandlesToServers = new HashMap<String, Vector<Location>>();
		filesThatHaveBeenDeleted = new Vector<String>();
		
		//read all metadata from files on startup
		readMetaData();
		ServerSocket ss = null;
		try{
			ss = new ServerSocket(43317);//find open socket
			while (true)
			{
				Socket s = ss.accept();
				//System.out.println("Client connected to master");
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
		try {
			fr = new FileReader(nameSpaceFile);
			br = new BufferedReader(fr);
			
			String temp = br.readLine();
			while(temp != null){
				namespace.add(temp);//add each entry
				//System.out.println("Constructor added: "+temp);
				temp = br.readLine();//read each entry from file
			}
			//System.out.println("After initialization from file namespace size: "+namespace.size());
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
	/*public void readChunksToLocations()
	{
		
		FileReader fr;
		BufferedReader br;
		//read the mapping of chunkHandles to their host Servers
		//will be in the format ChunkHandle:ServerIP,Port,ServerIP2,Port2
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
	}*/
	public void readMetaData()
	{
		readNameSpace();
		readFilesToChunks();
		//readChunksToLocations();
	}
	public void applyLog()
	{
		if (currentLogFile == null)//if the master is starting up fresh
		{
			File logFile = new File("logfile1.txt");//create a new logfile
			currentLogFile ="logfile1.txt";//set the currentLogFile
		}
		try {
			FileReader fr = new FileReader(currentLogFile);
			BufferedReader br = new BufferedReader(fr);
				
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
	
	//Returns the handles which have been deleted
	public Vector<String> updateChunkLocations(Location loc, String [] ChunkHandles)
		{
			Vector<String> deletedChunks = new Vector<String>();
			boolean newLocation = true;
			for(int i = 0; i < ChunkHandles.length; i++)
			{
				
				Vector<Location> ServerVector = chunkHandlesToServers.get(ChunkHandles[i]);
				for(int j = 0; j < ServerVector.size(); j++)
				{
					Location location = ServerVector.elementAt(j);
					if(location.IPAddress.equals(loc.IPAddress))
					{
						newLocation = false;
						break;
					}
				}
				if(newLocation)
				{
					ServerVector.add(loc);
				}
				newLocation = true;
				if(filesThatHaveBeenDeleted.contains(ChunkHandles[i]))
				{
					deletedChunks.add(ChunkHandles[i]);
				}
			}
			return deletedChunks;
		}
		
		public boolean renewLease(Location loc, String ChunkHandle)
		{
			Lease lease = ChunkLeaseMap.get(ChunkHandle);
			if(loc.equals(LeaseServerMap.get(lease)))
			{
				lease.updateLeaseMaster();
				return true;
			}
			
			return false;
		}
	
	public static void main(String[] args){
		TFSMaster master = new TFSMaster();
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
			try{
				while (true)
				{
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
					if (command.equals("CreateFile"))
					{
						createFile();
					}
					if (command.equals("DeleteFile"))
					{
						deleteFile();
					}
					if (command.equals("ListDir"))
					{
						listDir();
					}
				}
			}
			catch (ClassNotFoundException e) {
					e.printStackTrace();
			}
			catch (IOException e) {
					e.printStackTrace();
			}
		}
		public void createDir() throws IOException, ClassNotFoundException
		{
			//check if the src doesn't exist
			String srcDirectory = (String) ois.readObject();
			//System.out.println("Start - Source is: " + srcDirectory);
			boolean checkSrcExists = namespace.contains(srcDirectory);//if this returns null, there is no match
			if (!checkSrcExists) {
				oos.writeObject("does_not_exist");
				oos.flush();
				System.out.println(srcDirectory +" does_not_exist");
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			
			//check if directory exists
			String dirname = (String) ois.readObject();
			boolean checkDirExists = namespace.contains(srcDirectory+"/"+dirname);//if this returns null, there is no match
			if (checkDirExists) {
				oos.writeObject("dir_exists");
				oos.flush();
				System.out.println("dir_exists");
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//append this create operation to the logfile
			if(master.currentLogFile == null) {
				System.out.println("Cannot append to log, current log == null");
				
			}
			FileOutputStream fos = new FileOutputStream(master.currentLogFile);
			PrintWriter pw = new PrintWriter(fos); 
			pw.println("createDir:"+srcDirectory+":"+dirname);//create log record of create operation
			pw.close();
			
			//create the directory in the namespace
			namespace.add(srcDirectory+dirname+"/");
			//System.out.println("Added: "+srcDirectory+dirname+" to namespace");
			
			
			
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
				boolean checkDirExists = namespace.contains(srcDirectory+dirname+"/");//if this returns null, there is no match
				if (!checkDirExists) {
					System.out.println(srcDirectory+dirname + "/"+" doesn't exist");
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
				
				int directoriesFoundToDelete = 0;
				Iterator it = (Iterator) namespace.iterator();
				while (it.hasNext())
				{
					//check if src/dest is a substring
					String toCheck = (String) it.next();
					//System.out.println("checking if: " + toCheck+ " begins w/ " + srcDirectory+dirname);
					if (toCheck.startsWith(srcDirectory+dirname))
					{
						//if its a match add to list of deleted (will be sent to ChunkServers via heartbeat message)
						//then delete it from the namespace
						filesThatHaveBeenDeleted.add(toCheck);
						directoriesFoundToDelete++;
						
						//log the delete operation
						//append this create operation to the logfile
						if(master.currentLogFile == null) System.out.println("Cannot append to log, current log == null");
						FileOutputStream fos = new FileOutputStream(master.currentLogFile);
						PrintWriter pw = new PrintWriter(fos); 
						pw.println("deleteDir:"+srcDirectory+"/"+dirname);//create log record of create operation
						pw.close();
						
						//remove from namespace
						it.remove();
					}
				}
				//System.out.println(directoriesFoundToDelete);
				//send response to ClientFS
				if (directoriesFoundToDelete > 1)
				{
					oos.writeObject("success_dir_not_empty");
					oos.flush();
				}
				else{
					oos.writeObject("success");
					oos.flush();
				}
				
			}catch (IOException IOE){
				IOE.printStackTrace();
			}catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		public void renameDir() throws ClassNotFoundException, IOException
		{
			String src = (String) ois.readObject();
			System.out.println("Master RenameDir Src: "+src);
			
			Iterator it = namespace.iterator();
			while (it.hasNext())
			{
				String temp = (String) it.next();
				System.out.println(temp);
			}
			
			
			//check if source exists
			boolean checkSrcExists = (namespace.contains(src) || namespace.contains(src+"/"));
			if (!checkSrcExists)
			{
				System.out.println(src+" and "+src+"/ both don't exist in namespace");
				
				System.out.println("Begin List Namespace:");
				it = namespace.iterator();
				while (it.hasNext())
				{
					String temp = (String) it.next();
					System.out.println(temp);
				}
				System.out.println("End List Namespace:");
				oos.writeObject("does_not_exist");
				oos.flush();
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			String newName = (String) ois.readObject();
			//check if the directory already exists in the namespace
			boolean checkNewNameExists = (namespace.contains(src+"/"+newName));
			if (checkNewNameExists)
			{
				oos.writeObject("dest_dir_exits");
				oos.flush();
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//remove the old directory from the namespace and rename
			//must also rename any directory beginning w/ src/oldName
			/*Iterator*/ it = namespace.iterator();
			System.out.println("Finding directory paths that start with: " + src);
			while (it.hasNext())
			{
				String temp = (String) it.next();//iterate through each namespace entry
				if (temp.startsWith(src))
				{
					int srcLength = src.length();//get the length of the sourceDir path
					srcLength++; //to account for /
					
					//store everything after src/
					//example - from src/a/b/c/d/e/f.txt get /b/c/d/e/f.txt
					String afterSrc = temp.substring(srcLength, temp.length());
					
					//add the renamed path
					String renamedPath = newName+afterSrc;
					System.out.println("Adding renamed path: "+renamedPath);
					
					//remove the old entry from the namespace
					it.remove();
					
				}
			}
			//send confirmation back to ClientFS
			oos.writeObject("success");
			oos.flush();

		}
		public void listDir() throws ClassNotFoundException, IOException
		{
			//receive path of directory to list contents
			String target = (String) ois.readObject();
			//check if it exists first
			boolean checkTargetExists = (namespace.contains(target)||namespace.contains(target+"/"));
			if (!checkTargetExists)
			{
				oos.writeObject("does_not_exist");
				oos.flush();
				System.out.println(target +" does_not_exist");
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			//check if the directory is empty
			Iterator it = namespace.iterator();
			//create vector of all contents of this directory
			Vector<String> contents = new Vector<String>();
			while (it.hasNext())
			{
				String temp = (String)it.next();
				if (temp.startsWith(target))//if it is a match -- WONT THIS GET ALL THE SUBFOLDERS OF SUBFOLDERS
				{
					contents.addElement(temp);
				}
			}
			//if the contents only has 1 match, the directory is empty
			if(contents.size() == 1)
			{
				oos.writeObject("is_empty");
				//System.out.println("the directory is empty!");
				oos.flush();
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//send the contents
			oos.writeObject(contents);
			oos.flush();
			
		}
		public void openFile()
		{
			try {
				//read which file wants to be opened
				String filePath = (String) ois.readObject();
				
				
				//use lookup table to get handles of all chunks of that file
				Vector<String> chunksOfFile = filesToChunkHandles.get(filePath);
				
				if(chunksOfFile==null){
					//send confirmation that file does not exist
					oos.writeObject("file_does_not_exist");
					oos.flush();
					return;
				}
				else{
					//send confirmation that file exists
					oos.writeObject("file_exists");
					oos.flush();
				}
				
				//send back to ClientFS to be loaded into the Filehandle object
				oos.writeObject(chunksOfFile);
				oos.flush();
				
				//for each chunk, get its location -- IP address of chunkserver
				//maps chunkHandles to all the locations of its replicas
				HashMap<String, Vector<Location>> ChunkLocations = new HashMap<String, Vector<Location>>();
				for (int i = 0; i < chunksOfFile.size(); i++)
				{
					Vector<Location> location = chunkHandlesToServers.get(chunksOfFile.elementAt(i));
					ChunkLocations.put(chunksOfFile.elementAt(i), location);
				}
				
				//send that array back to ClientFS to load into FileHandle object
				oos.writeObject(ChunkLocations);
				oos.flush();
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}		}
		public void closeFile()
		{
			try {
				//read which file wants to be opened
				FileHandle fileHandle = (FileHandle) ois.readObject();
				
				String filePath = fileHandle.getFileName();
				
				//use lookup table to get handles of all chunks of that file
				Vector<String> chunksOfFile = filesToChunkHandles.get(filePath);
				
				if(chunksOfFile==null){
					//send confirmation that file does not exist or is invalid
					oos.writeObject("invalid");
					oos.flush();
					return;
				}
				else{
					//send confirmation that file exists
					oos.writeObject("file_exists");
					oos.flush();
				}
				
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void createFile()
		{
			try{
				//check if the src doesn't exist
				String tgtdir = (String) ois.readObject();
				
				//System.out.println("Start - Source is: " + srcDirectory);
				boolean checkSrcExists = namespace.contains(tgtdir);//if this returns null, there is no match
				if (!checkSrcExists) {
					oos.writeObject("does_not_exist");
					oos.flush();
					System.out.println(tgtdir +" does_not_exist");
					return;
				}
				else{
					oos.writeObject("x"); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
			
			
				//check if file already exists
				String fileName = (String) ois.readObject();
				boolean checkFileExists = namespace.contains(tgtdir+"/"+fileName);//if this returns null, there is no match
				if (checkFileExists) {
					oos.writeObject("file_exists");
					oos.flush();
					System.out.println("file_exists");
					return;
				}
				else{
					//add the file to the namespace
					namespace.add(tgtdir+fileName);
					
					//send confirmation
					oos.writeObject("success");
					oos.flush();
				}
			
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
			}
		}
		public void deleteFile()
		{
			try{
				//check if the src doesn't exist
				String tgtdir = (String) ois.readObject();
				
				//System.out.println("Start - Source is: " + srcDirectory);
				boolean checkSrcExists = namespace.contains(tgtdir);//if this returns null, there is no match
				if (!checkSrcExists) {
					oos.writeObject("src_does_not_exist");
					oos.flush();
					System.out.println(tgtdir +" src_does_not_exist");
					return;
				}
				else{
					oos.writeObject("x"); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
			
			
				//check if file already exists
				String fileName = (String) ois.readObject();
				boolean checkFileExists = namespace.contains(tgtdir+"/"+fileName);//if this returns null, there is no match
				if (!checkFileExists) {
					oos.writeObject("file_does_not_exist");
					oos.flush();
					System.out.println("file_does_not_exist");
					return;
				}
				else{
					//remove the file from the namespace
					namespace.remove(tgtdir+fileName);
					
					//send confirmation
					oos.writeObject("success");
					oos.flush();
				}
			
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
			}
		}
	}



}
