package com.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.chunkserver.ClientInstance;
import com.client.Client;

import master.Location;
import master.TFSMaster;

//TODO connect sockets during each operation depending on where the chunkserver is located
// Can't connect in instructor because we do not know the location that we want to connect to

public class ClientRec {
	static Socket MasterConnection;
	static ObjectOutputStream WriteOutputMaster;
	static ObjectInputStream ReadInputMaster;
	public static String MasterIPAddress;
	public static  int MasterPort;
	
	public ClientRec() { //Will have to change. 
						 //The client will connect to different chunk servers depending on which one as the data
		if (MasterConnection != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(TFSMaster.MasterConfigFile));
			String portAndIP = binput.readLine();
			StringTokenizer str = new StringTokenizer(portAndIP,":");
			MasterIPAddress = str.nextToken();//get the master's ip addres
			MasterPort = Integer.parseInt(str.nextToken());//get the port that master listeningon;
			
			//MasterConnection = new Socket("127.0.0.1", MasterPort); //should client be reading from config?
			//WriteOutputMaster = new ObjectOutputStream(MasterConnection.getOutputStream());
			//ReadInputMaster = new ObjectInputStream(MasterConnection.getInputStream());
		}catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ TFSMaster.MasterConfigFile +" containing the port of the ChunkServer is missing.");
		}catch (IOException e) {
			System.out.println("Can't find file.-1");
		}
	}
	
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		if(ofh == null) {
			System.out.println("ofh null in AppendRecord");
			return FSReturnVals.BadHandle;
		}
		if(RecordID.ChunkHandle != null) {
			System.out.println("rid.chuknhandle null in AppendRecord");
			return FSReturnVals.BadRecID;
		}
		if(RecordID.index < 0)
		{
			System.out.println("index < 1 in AppendRecord");
		}
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		byte[] CHinBytes;
		Vector<Location> Locations = ofh.getLocations();
		int count = 0;
		int size;
		String ChunkHandle = "";
		for(int i = 0; i < Locations.size(); i++) {
			try {
				Location primaryLoc = Locations.get(i);
				System.out.println("primaryLoc: " + primaryLoc.IPAddress + " - " + primaryLoc.port);
				Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
				ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
				WriteOutputCS.flush();
				ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
				
				if(ChunkHandles.isEmpty())
				{
					System.out.println("Client rec is attempting to createChunk");
					WriteOutputCS.writeInt(ChunkServer.CreateChunkCMD);
					WriteOutputCS.flush();
					WriteOutputCS.writeObject(Locations);
					WriteOutputCS.flush();
					size = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
					CHinBytes = Client.RecvPayload("ClientRec", ReadInputCS, size);
					ChunkHandle = new String(CHinBytes);
					ChunkHandles.add(ChunkHandle);
					ofh.setHandles(ChunkHandles);
					
				}
				else
				{
					ChunkHandle = ChunkHandles.lastElement();
				}
				while(count < 2)
				{
					CHinBytes = ChunkHandle.getBytes();
					WriteOutputCS.writeInt(ChunkServer.WriteChunkCMD); //Code
					WriteOutputCS.writeInt(payload.length);
					WriteOutputCS.write(payload);
					WriteOutputCS.writeInt(CHinBytes.length);
					WriteOutputCS.write(CHinBytes); //ChunkHandle
					WriteOutputCS.flush();
					WriteOutputCS.writeObject(Locations);
					WriteOutputCS.flush();
					int index = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
					if(index == -1 && count == 0)
					{
						WriteOutputCS.writeInt(ChunkServer.CreateChunkCMD);
						WriteOutputCS.flush();
						WriteOutputCS.writeObject(Locations);
						WriteOutputCS.flush();
						size = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
						CHinBytes = Client.RecvPayload("ClientRec", ReadInputCS, size);
						ChunkHandle = new String(CHinBytes);
						ChunkHandles.add(ChunkHandle);
						ofh.setHandles(ChunkHandles);
					}
					else if(index == -1 && count == 1)
					{
						WriteOutputCS.writeInt(ClientInstance.CloseSockets);
						WriteOutputCS.flush();
						ReadInputCS.close();
						WriteOutputCS.close();
						CSConnection.close();
						return FSReturnVals.Fail;
					}
					else
					{
						RecordID = new RID();
						RecordID.ChunkHandle = ChunkHandle;
						RecordID.index = index;
						WriteOutputCS.writeInt(ClientInstance.CloseSockets);
						WriteOutputCS.flush();
						ReadInputCS.close();
						WriteOutputCS.close();
						CSConnection.close();
						return FSReturnVals.Success;
					}
					count++;
				}
				WriteOutputCS.writeInt(ClientInstance.CloseSockets);
				WriteOutputCS.flush();
				ReadInputCS.close();
				WriteOutputCS.close();
				CSConnection.close();
				return FSReturnVals.Fail;
			}		
			catch (IOException e) {
				// Get new primary loc
				// restart process
				e.printStackTrace();
				System.out.println("Moving to next ChunkServer: Append");
				
			}	
		}
		RecordID = null;
		return FSReturnVals.Fail;
			
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		if(ofh == null) { //how do we determine if a file handle is invalid?
			return FSReturnVals.BadHandle;
		}
		if(RecordID == null || RecordID.index == -1) { //how do we determine if a RID is invalid
			return FSReturnVals.BadRecID;
		}
		String ChunkHandle = RecordID.ChunkHandle;
		byte[] CHinBytes = ChunkHandle.getBytes();

		Vector<Location> Locations = ofh.getLocations();
		for(int i = 0; i < Locations.size(); i++) {
			try {
				Location primaryLoc = Locations.get(i);
				Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
				ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
				WriteOutputCS.flush();
				ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
				
			//	WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
				WriteOutputCS.writeInt(ClientInstance.DeleteRecord);
				WriteOutputCS.writeInt(RecordID.index);
				WriteOutputCS.writeInt(CHinBytes.length);
				WriteOutputCS.write(CHinBytes);
				WriteOutputCS.flush();
				
				int response = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
				if(response == ChunkServer.FALSE) {
					WriteOutputCS.writeInt(ClientInstance.CloseSockets);
					WriteOutputCS.flush();
					ReadInputCS.close();
					WriteOutputCS.close();
					CSConnection.close();
					return FSReturnVals.Fail;
				} else {
					WriteOutputCS.writeInt(ClientInstance.CloseSockets);
					WriteOutputCS.flush();
					ReadInputCS.close();
					WriteOutputCS.close();
					CSConnection.close();
					return FSReturnVals.Success;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
				System.out.println("Moving to next ChunkServer: Delete");
			}	
		}
		
		
		return null;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, rec, recid)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec tRec) {
		
		if(ofh.ChunkHandles.size() == 0) {
			System.out.println("ofh null in ReadFirstRecord");
			return FSReturnVals.BadHandle;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle = "";
		byte[] CHinBytes;
		RID RecordID;
		byte [] payload;
		Vector<Location> Locations = ofh.getLocations();
		for(int i = 0; i < Locations.size(); i++) {
			try {
				Location primaryLoc = Locations.get(i);
				Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
				ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
				ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
				int index = 0;
				for(int j = 0; j < ChunkHandles.size(); j++)
				{
					ChunkHandle = ChunkHandles.elementAt(j);
					CHinBytes = ChunkHandle.getBytes();
					//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
					WriteOutputCS.writeInt(ClientInstance.ReadFirstRecord);
					
					WriteOutputCS.writeInt(CHinBytes.length);
					WriteOutputCS.write(CHinBytes);
					WriteOutputCS.flush();		
					index = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
					if(index != -1) {
						int size = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
						payload = Client.RecvPayload("Client", ReadInputCS, size);

						RecordID = new RID();
						RecordID.index = index;
						RecordID.ChunkHandle = ChunkHandle; 
						tRec.setPayload(payload);
						tRec.setRID(RecordID);
						WriteOutputCS.writeInt(ClientInstance.CloseSockets);
						WriteOutputCS.flush();
						ReadInputCS.close();
						WriteOutputCS.close();
						CSConnection.close();
						System.out.println("Success reading first record");
						return FSReturnVals.Success;
					}
				}
				WriteOutputCS.writeInt(ClientInstance.CloseSockets);
				WriteOutputCS.flush();
				ReadInputCS.close();
				WriteOutputCS.close();
				CSConnection.close();
				return FSReturnVals.RecDoesNotExist;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
				System.out.println("Moving to next ChunkServer: readfirst");
			
			}	
		}
		return FSReturnVals.Fail;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, rec, recid)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec tRec) {
		if(ofh.ChunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle = "";
		byte[] CHinBytes;
		RID RecordID;
		byte [] payload;
		Vector<Location> Locations = ofh.getLocations();
		for(int i = 0; i < Locations.size(); i++) {
			try {
				Location primaryLoc = Locations.get(i);
				Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
				ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
				WriteOutputCS.flush();
				ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
				
				for(int j = ChunkHandles.size()-1; j >= 0; j--){
					ChunkHandle = ChunkHandles.elementAt(j);
					CHinBytes = ChunkHandle.getBytes();
					
					//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
					WriteOutputCS.writeInt(ClientInstance.ReadLastRecord);
					WriteOutputCS.writeInt(CHinBytes.length);
					WriteOutputCS.write(CHinBytes);
					WriteOutputCS.flush();
					
					int index =  Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
					if(index != -1) {
						int size = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
						payload = Client.RecvPayload("Client", ReadInputCS, size);

						RecordID = new RID();
						RecordID.index = index;
						RecordID.ChunkHandle = ChunkHandle; 
						tRec.setPayload(payload);
						tRec.setRID(RecordID);
						WriteOutputCS.writeInt(ClientInstance.CloseSockets);
						WriteOutputCS.flush();
						ReadInputCS.close();
						WriteOutputCS.close();
						CSConnection.close();
						return FSReturnVals.Success;
					}
				}
				WriteOutputCS.writeInt(ClientInstance.CloseSockets);
				WriteOutputCS.flush();
				ReadInputCS.close();
				WriteOutputCS.close();
				CSConnection.close();
				return FSReturnVals.RecDoesNotExist;
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
				System.out.println("Moving to next ChunkServer: readlast");
			}	
		}
		return FSReturnVals.Fail;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, rec, rec1) 2. ReadNextRecord(FH1,
	 * rec1, rec, rec2) 3. ReadNextRecord(FH1, rec2, rec, rec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec tRec) {
		if(ofh.ChunkHandles.size() == 0) {
			System.out.println("ofh null in RextNextRecord");
			return FSReturnVals.BadHandle;
		}
		if(pivot.ChunkHandle == null)
		{
			System.out.println("pivot.chunkhandle null in ReadNextRecord");
		}
		if(pivot.index < 0) {
			System.out.println("pivot.index < 0 in ReadNextRecord");
			return FSReturnVals.RecDoesNotExist;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		int indexOfChunkHandle = ofh.ChunkHandles.indexOf(pivot.ChunkHandle);
		String ChunkHandle = "";
		byte[] CHinBytes;
		RID RecordID;
		byte [] payload;
		int index = pivot.index + 1;
		Vector<Location> Locations = ofh.getLocations();
		for(int i = 0; i < Locations.size(); i++) {
			try {
				Location primaryLoc = Locations.get(i);

				Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
				ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
				ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
				
				for(int j = indexOfChunkHandle; j < ChunkHandles.size(); j++) {
					//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
					ChunkHandle = ChunkHandles.get(j);
					CHinBytes = ChunkHandle.getBytes();
					if(j != indexOfChunkHandle)
					{
						index = 0;
					}
					WriteOutputCS.writeInt(ClientInstance.ReadNextRecord);		
					WriteOutputCS.writeInt(index); //should this be +1?
					WriteOutputCS.writeInt(CHinBytes.length);
					WriteOutputCS.write(CHinBytes);
					WriteOutputCS.flush();
					
					index = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
					
					if(index != -1) {
						
						int size = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
						payload = Client.RecvPayload("ClientRec", ReadInputCS, size);
						
						RecordID = new RID();
						RecordID.index = index;
						RecordID.ChunkHandle = ChunkHandle;
						tRec.setPayload(payload);
						tRec.setRID(RecordID);
						WriteOutputCS.writeInt(ClientInstance.CloseSockets);
						WriteOutputCS.flush();
						ReadInputCS.close();
						WriteOutputCS.close();
						CSConnection.close();
						return FSReturnVals.Success;
					}
				}
				System.out.println("Record didn't exist");
				WriteOutputCS.writeInt(ClientInstance.CloseSockets);
				WriteOutputCS.flush();
				ReadInputCS.close();
				WriteOutputCS.close();
				CSConnection.close();
				return FSReturnVals.RecDoesNotExist;
				
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block

				e.printStackTrace();
				System.out.println("Moving to next ChunkServer: readnext");
			}
			return FSReturnVals.RecDoesNotExist;
			
		}
		return FSReturnVals.Fail;
		
		
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, rec, recn) 2. ReadPrevRecord(FH1,
	 * recn-1, rec, rec2) 3. ReadPrevRecord(FH1, recn-2, rec, rec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec tRec) {
		if(ofh.ChunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		if(pivot.index < 0) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		int indexOfChunkHandle = ofh.ChunkHandles.indexOf(pivot.ChunkHandle);
		String ChunkHandle = "";
		byte[] CHinBytes;
		RID RecordID;
		byte [] payload;
		int index = pivot.index - 1;
		if(index == -1)
		{
			indexOfChunkHandle--;
		}
		Vector<Location> Locations = ofh.getLocations();
		for(int i = 0; i < Locations.size(); i++) {
			try {
				Location primaryLoc = Locations.get(i);

				Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
				ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
				WriteOutputCS.flush();
				ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
				
				for(int j = indexOfChunkHandle; j >= 0; j--) {
					ChunkHandle = ChunkHandles.get(j);
					CHinBytes = ChunkHandle.getBytes();
					if(j != indexOfChunkHandle)
					{
						index = -1;
					}
					//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
					WriteOutputCS.writeInt(ClientInstance.ReadPreviousRecord);		
					WriteOutputCS.writeInt(index); //should this be - 1?
					WriteOutputCS.writeInt(CHinBytes.length);
					WriteOutputCS.write(CHinBytes);
					WriteOutputCS.flush();
					
					index = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
					
					if(index != -1) {
						int size = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
						payload = Client.RecvPayload("ClientRec", ReadInputCS, size);
						
						RecordID = new RID();
						RecordID.index = index;
						RecordID.ChunkHandle = ChunkHandle;
						tRec.setPayload(payload);
						tRec.setRID(RecordID);
						WriteOutputCS.writeInt(ClientInstance.CloseSockets);
						WriteOutputCS.flush();
						ReadInputCS.close();
						WriteOutputCS.close();
						CSConnection.close();
						return FSReturnVals.Success;
					}		
					System.out.println("Next chunkhandle");
				}
				WriteOutputCS.writeInt(ClientInstance.CloseSockets);
				WriteOutputCS.flush();
				ReadInputCS.close();
				WriteOutputCS.close();
				CSConnection.close();
				return FSReturnVals.RecDoesNotExist;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
				System.out.println("Moving to next ChunkServer: readlast");
			}
		}
		return FSReturnVals.Fail;

	}
	
	public Vector<Location> getNewLocations()
	{
		try {
			BufferedReader binput = new BufferedReader(new FileReader(TFSMaster.MasterConfigFile));
			String portAndIP = binput.readLine();
			StringTokenizer str = new StringTokenizer(portAndIP,":");
			String masterIP = str.nextToken();//get the master's ip addres
			int ServerPort = Integer.parseInt(str.nextToken());//get the port that master listeningon
			
			//port = port.substring( port.indexOf(':')+1 );
			//ServerPort = Integer.parseInt(port);
			Socket ClientSocket = new Socket(masterIP,ServerPort);//should client be reading from config?
			
			ObjectOutputStream WriteOutput = new ObjectOutputStream(ClientSocket.getOutputStream());
			ObjectInputStream ReadInput = new ObjectInputStream(ClientSocket.getInputStream());
			WriteOutput.writeObject("clientFS");
			WriteOutput.flush();
			WriteOutput.writeObject("getNewDirs");
			WriteOutput.flush();
			Vector<Location> newLocations = (Vector<Location>)ReadInput.readObject();
			return newLocations;
		}
		catch(ClassNotFoundException cnfe)
		{
			cnfe.printStackTrace();
		}
		catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ TFSMaster.MasterConfigFile +" containing the port of the ChunkServer is missing.");
		}catch (IOException e) {
			System.out.println("Can't find file.");
		}
		return null;
	}
	
}
