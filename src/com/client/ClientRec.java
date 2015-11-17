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
	public static final String MasterIPAddress = "127.0.0.1";
	public static final int MasterPort = 43317;
	
	public ClientRec() { //Will have to change. 
						 //The client will connect to different chunk servers depending on which one as the data
		if (MasterConnection != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(ChunkServer.ClientConfigFile));
			String port = binput.readLine();
			port = port.substring( port.indexOf(':')+1 );
			int ServerPort = Integer.parseInt(port);
			
			//MasterConnection = new Socket("127.0.0.1", MasterPort); //should client be reading from config?
			//WriteOutputMaster = new ObjectOutputStream(MasterConnection.getOutputStream());
			//ReadInputMaster = new ObjectInputStream(MasterConnection.getInputStream());
		}catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ ChunkServer.ClientConfigFile +" containing the port of the ChunkServer is missing.");
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
			return FSReturnVals.BadHandle;
		}
		if(RecordID.ChunkHandle != null) {
			return FSReturnVals.BadRecID;
		}
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		byte[] CHinBytes;
		Location primaryLoc = ofh.getPrimaryLocation();
		int count = 0;
		int size;
		try {
			//System.out.println("InCLientRec");
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			//System.out.println("Opened up socket");
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			//System.out.println("Opened up socket and streams1");
			WriteOutputCS.flush();
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			//System.out.println("Opened up socket and streams2");
			String ChunkHandle;
			if(ChunkHandles.isEmpty())
			{
				System.out.println("CLient rec is attempting to createChunk");
				WriteOutputCS.writeInt(ChunkServer.CreateChunkCMD);
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
				
				int index = Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
				if(index == -1 && count == 0)
				{
					WriteOutputCS.writeInt(ChunkServer.CreateChunkCMD);
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
			// TODO Auto-generated catch block
			RecordID = null;
			
			return FSReturnVals.Fail;
		}
			
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
		Location primaryLoc = ofh.getPrimaryLocation();
		
		try {
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
			return FSReturnVals.BadHandle;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle;
		byte[] CHinBytes;
		Location primaryLoc = ofh.getPrimaryLocation();
		RID RecordID;
		byte [] payload;
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			int index = 0;
			for(int i = 0; i < ChunkHandles.size(); i++)
			{
				ChunkHandle = ChunkHandles.elementAt(i);
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
			return FSReturnVals.Fail;
		}
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
		String ChunkHandle;
		byte[] CHinBytes;
		Location primaryLoc = ofh.getPrimaryLocation();
		RID RecordID;
		byte [] payload;
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			WriteOutputCS.flush();
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			for(int i = ChunkHandles.size()-1; i >= 0; i--){
				ChunkHandle = ChunkHandles.elementAt(i);
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
			return FSReturnVals.Fail;
		}
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
			return FSReturnVals.BadHandle;
		}
		if(pivot.index < 0) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		int indexOfChunkHandle = ofh.ChunkHandles.indexOf(pivot.ChunkHandle);
		String ChunkHandle;
		byte[] CHinBytes;
		Location primaryLoc = ofh.getPrimaryLocation();
		RID RecordID;
		byte [] payload;
		int index = pivot.index + 1;
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			for(int i = indexOfChunkHandle; i < ChunkHandles.size(); i++) {
				//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
				ChunkHandle = ChunkHandles.get(i);
				CHinBytes = ChunkHandle.getBytes();
				if(i != indexOfChunkHandle)
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
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return FSReturnVals.Fail;
		}
		
		
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
		String ChunkHandle;
		byte[] CHinBytes;
		Location primaryLoc = ofh.getPrimaryLocation();
		RID RecordID;
		byte [] payload;
		int index = pivot.index - 1;
		if(index == -1)
		{
			indexOfChunkHandle--;
		}
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			WriteOutputCS.flush();
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			for(int i = indexOfChunkHandle; i >= 0; i--) {
				ChunkHandle = ChunkHandles.get(i);
				CHinBytes = ChunkHandle.getBytes();
				if(i != indexOfChunkHandle)
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
			return FSReturnVals.Fail;
		}
	}


	
}
