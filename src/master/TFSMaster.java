package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import java.util.LinkedHashSet;
import java.util.StringTokenizer;
import java.util.Vector;





import com.chunkserver.ChunkServer;
import com.chunkserver.Lease;
import com.client.FileHandle;

public class TFSMaster{
	
	public final static String MasterConfigFile = "MasterConfig.txt";
	public static String currentLogFile;
	public static Vector<String> filesThatHaveBeenDeleted;
	
	public static LinkedHashSet<String> namespace; //maps directory paths to IP address of chunk servers
	public static LinkedHashMap<String, Vector<String>> filesToChunkHandles; // maps which chunks constitute a file
	public static HashMap<String, Vector<Location>> chunkHandlesToServers; //maps chunk handles to locations of their replicas(CS IP addresses)
	public static HashMap<String, Lease> ChunkLeaseMap;
	public static HashMap<Lease, String> LeaseServerMap;
	
	
	public static final String nameSpaceFile = "namespace.txt";
	public static final String filesToChunkHandlesFile = "filesToChunkHandles.txt";
	public static final String chunkHandlesToServersFile = "chunkHandlesToServers.txt";
	public static int logSize = 0; public static int logNumber;
	
	public TFSMaster()
	{
		namespace = new LinkedHashSet<String>();
		filesToChunkHandles = new LinkedHashMap<String, Vector<String>>();
		chunkHandlesToServers = new HashMap<String, Vector<Location>>();
		filesThatHaveBeenDeleted = new Vector<String>();
		
		//read all metadata from files on startup
		readMasterConfig();
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
	public void readMasterConfig()
	{
		FileReader fr;
		BufferedReader br;
		try{
			fr = new FileReader(MasterConfigFile);
			br = new BufferedReader(fr);
			
			//read the number of the log file that is most current
			String logNum = br.readLine();
			this.logNumber = Integer.parseInt(logNum);
			
			//set the current log file
			this.currentLogFile = "log"+logNumber+".txt";
			
			br.close();fr.close();
		}catch (FileNotFoundException e) {
			System.out.print("FNFE while reading MasterConfig info");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.print("IOE while reading MasterConfig info");
			e.printStackTrace();
		}
		
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
				String temp = br.readLine();
				while(temp != null)
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
					temp = br.readLine();
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
		try {
			FileReader fr = new FileReader(currentLogFile);
			BufferedReader br = new BufferedReader(fr);
			String logLine = br.readLine();
			//read through logfile and apply operations
			//should be in format: create:srcDirectoryName:directoryToCreateName
			while (logLine!= null)
			{
				StringTokenizer str = new StringTokenizer(logLine);
				String command = str.nextToken();//the first token is the command
				
				if (command.equals("createDir"))
				{
					createFromLog(str);
				}
				if (command.equals("renameDir"))
				{
					renameFromLog(str);
				}
				if (command.equals("deleteDir"))
				{
					deleteFromLog(str);
				}
				if (command.equals("createFile"))
				{
					
				}
				if (command.equals("deleteFile"))
				{
						
				}
				logLine = br.readLine();//read each line of the log
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
		
		//create the directory in the namespace
		namespace.add(src+directoryToCreateName+"/");
		
		//add the information to the persistent namespace file
		try {
			FileWriter fw = new FileWriter(nameSpaceFile);
			PrintWriter pw = new PrintWriter(fw);
			pw.println(src+directoryToCreateName+"/");
			pw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public void deleteFromLog(StringTokenizer str)
	{
		String src = str.nextToken();
		String directoryToDelete = str.nextToken();
		
		//remove the entry from namespace in main memory
		namespace.remove(src+directoryToDelete);
		
		//apply the changes to the persistent file
		try {
			
			//update the namespace file by writing it out to temp minus the deleted namespace
			File oldnamespace = new File(nameSpaceFile);
			File newnamespace = new File("temp-namespace.txt");
			
			BufferedReader br = new BufferedReader(new FileReader(oldnamespace));
			BufferedWriter bw = new BufferedWriter(new FileWriter(newnamespace));
			
			//the entry that was deleted
			String searchingFor = src+directoryToDelete;
			String currentLine;
			
			while ((currentLine = br.readLine())!= null)
			{
				if (currentLine.equals(searchingFor))continue; //skip it if its supposed to be deleted
				bw.write(currentLine+System.getProperty("line.separator"));//write with an endline separator
			}
			
			bw.close();
			br.close();
			oldnamespace.delete(); //delete the oldnamespacefile
			boolean success = newnamespace.renameTo(oldnamespace);//rename it back to the old namespace.txt file
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	public void renameFromLog(StringTokenizer str){
		
		String src = str.nextToken();
		String newName = str.nextToken();
		
		Vector<String> newNamestoAdd = new Vector<String>();
		Iterator it = namespace.iterator();
		while (it.hasNext())
		{
			String temp = (String) it.next();//iterate through each namespace entry
			if (temp.startsWith(src+"/"))
			{
				int srcLength = src.length();//get the length of the sourceDir path
				srcLength++; //to account for / character
				
				//store everything after src/
				//example - from src/a/b/c/d/e/f.txt get /b/c/d/e/f.txt
				String afterSrc = temp.substring(srcLength, temp.length());
				
				//add the renamed path
				String renamedPath = newName+"/"+afterSrc;
				newNamestoAdd.add(renamedPath);
				
				//remove the old entry from the namespace
				it.remove();
			}
		}
		
		//add back all the newly named paths to namespace
		for (int i = 0; i < newNamestoAdd.size(); i++)
		{
			namespace.add(newNamestoAdd.get(i));
		}
		
		//write out namespace to persistent file
		//update the namespace file by writing it out to temp minus the deleted namespace
		File newnamespace = new File("temp-namespace.txt");
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(newnamespace));
			
			it = namespace.iterator();
			while (it.hasNext())
			{
				String temp = (String) it.next();
				bw.write(temp+System.getProperty("line.separator"));//write each entry and newline char
			}
			
			bw.close();
			File oldnamespace = new File(nameSpaceFile);
			oldnamespace.delete(); //delete the oldnamespacefile
			boolean success = newnamespace.renameTo(oldnamespace);//assign the new file to namespace.txt
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
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
					if (command.equals("OpenFile"))
					{
						openFile();
					}
					if (command.equals("CloseFile"))
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
					if(command.equals("NameSpace")){
						NameSpace();
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
						directoriesFoundToDelete++;
					}
				}
				//send response to ClientFS
				if (directoriesFoundToDelete > 1)
				{
					oos.writeObject("dir_not_empty");
					oos.flush();
				}
				else{
					oos.writeObject("success");
					oos.flush();
					
					//write the delete to the log
					FileWriter fw = new FileWriter(currentLogFile,true);//open file in append only mode
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write("deleteDir:"+srcDirectory+":"+dirname+"/");
					bw.close();
					
					//remove the namespace from directory
					namespace.remove(srcDirectory+dirname+"/");
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
			Iterator it;

			boolean checkSrcExists = (namespace.contains(src) || namespace.contains(src+"/"));
			if (!checkSrcExists)
			{
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
			//System.out.println("Finding directory paths that start with: " + src);
			
			//Writes to logfile
			FileWriter fw = new FileWriter(currentLogFile,true);//open file in append only mode
			BufferedWriter bw = new BufferedWriter(fw);
			
			
			Vector<String> newNamestoAdd= new Vector<String>();
			it = namespace.iterator();
			while (it.hasNext())
			{
				String temp = (String) it.next();//iterate through each namespace entry
				if (temp.startsWith(src+"/"))
				{
					int srcLength = src.length();//get the length of the sourceDir path
					srcLength++; //to account for /
					
					//store everything after src/
					//example - from src/a/b/c/d/e/f.txt get /b/c/d/e/f.txt
					String afterSrc = temp.substring(srcLength, temp.length());
					
					//write to the log
					bw.write("renameDir:"+src+":"+newName);
					
					//add the renamed path
					String renamedPath = newName+"/"+afterSrc;
					newNamestoAdd.add(renamedPath);
					
					//remove the old entry from the namespace
					it.remove();
				}
			}bw.close();
			
			//add back all the newly named paths to namespace
			for (int i = 0; i < newNamestoAdd.size(); i++)
			{
				namespace.add(newNamestoAdd.get(i));
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
				String fileName = (String) ois.readObject();
				Vector<String> chunksOfFile = (Vector<String>) ois.readObject();
				
				//remove old mapping and add new key,value pair
				System.out.println("removing "+fileName+" mapping from master.");
				filesToChunkHandles.remove(fileName);
				filesToChunkHandles.put(fileName, chunksOfFile);
				System.out.println("Updated "+fileName+" mapping from master.");
				
				oos.writeObject("success");
				oos.flush();
				
				
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
		
		public void NameSpace() {
			//send confirmation
			try {
				//send namespace hashset
				oos.writeObject(namespace);
				oos.flush();
				
			} catch (IOException e) {
				e.printStackTrace();
		}
			
		}
	}
	
	



}
