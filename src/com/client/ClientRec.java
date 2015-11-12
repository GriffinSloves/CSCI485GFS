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
	public static final int MasterPort = 6789;
	
	public ClientRec() { //Will have to change. 
						 //The client will connect to different chunk servers depending on which one as the data
		if (MasterConnection != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(ChunkServer.ClientConfigFile));
			String port = binput.readLine();
			port = port.substring( port.indexOf(':')+1 );
			int ServerPort = Integer.parseInt(port);
			
			MasterConnection = new Socket("127.0.0.1", MasterPort); //should client be reading from config?
			WriteOutputMaster = new ObjectOutputStream(MasterConnection.getOutputStream());
			ReadInputMaster = new ObjectInputStream(MasterConnection.getInputStream());
		}catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ ChunkServer.ClientConfigFile +" containing the port of the ChunkServer is missing.");
		}catch (IOException e) {
			System.out.println("Can't find file.");
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
		if(RecordID != null) {
			return FSReturnVals.BadRecID;
		}
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle = ChunkHandles.lastElement();
		byte[] CHinBytes = ChunkHandle.getBytes();
		Location primaryLoc = ofh.getPrimaryLocation();
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			WriteOutputCS.writeInt(ChunkServer.WriteChunkCMD); //Code
			WriteOutputCS.writeInt(payload.length);
			WriteOutputCS.write(payload);
			WriteOutputCS.write(CHinBytes.length);
			WriteOutputCS.write(CHinBytes); //ChunkHandle
			WriteOutputCS.flush();
			
			int index = ReadInputCS.readInt();
			if(index == -1)
			{
				return FSReturnVals.Fail;
			}
			else
			{
				RecordID = new RID();
				RecordID.ChunkHandle = ChunkHandle;
				RecordID.index = index;
				return FSReturnVals.Success;
			}
			
		} catch (IOException e) {
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
		if(RecordID == null) { //how do we determine if a RID is invalid
			return FSReturnVals.BadRecID;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle = ChunkHandles.lastElement();
		byte[] CHinBytes = ChunkHandle.getBytes();
		Location primaryLoc = ofh.getPrimaryLocation();
		
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
		//	WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutputCS.writeInt(ClientInstance.DeleteRecord);
			WriteOutputCS.writeInt(RecordID.index);
			WriteOutputCS.writeInt(CHinBytes.length);
			WriteOutputCS.write(CHinBytes);
			WriteOutputCS.flush();
			
			int response = ReadInputCS.readInt();
			if(response == ChunkServer.FALSE) {
				return FSReturnVals.Fail;
			} else {
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
	public FSReturnVals ReadFirstRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		
		if(ofh.ChunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle = ChunkHandles.firstElement();
		byte[] CHinBytes = ChunkHandle.getBytes();
		Location primaryLoc = ofh.getPrimaryLocation();
		
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutputCS.writeInt(ClientInstance.ReadFirstRecord);
			
			WriteOutputCS.writeInt(CHinBytes.length);
			WriteOutputCS.write(CHinBytes);
			WriteOutputCS.flush();
			
			int ChunkSize =  Client.ReadIntFromInputStream("ClientRec", ReadInputCS);
			ChunkSize -= ChunkServer.PayloadSZ;  //reduce the length by the first four bytes that identify the length
			
			if(ChunkSize == 0) {
				return FSReturnVals.RecDoesNotExist;
			}
			
			payload = Client.RecvPayload("Client", ReadInputCS, ChunkSize);

			RecordID = new RID();
			RecordID.index = 1;
			RecordID.ChunkHandle = ChunkHandle; 
			
			return FSReturnVals.Success;
			
			
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
	public FSReturnVals ReadLastRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		if(ofh.ChunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		
		Vector<String> ChunkHandles = ofh.getChunkHandles();
		String ChunkHandle = ChunkHandles.lastElement();
		byte[] CHinBytes = ChunkHandle.getBytes();
		Location primaryLoc = ofh.getPrimaryLocation();
		
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutputCS.writeInt(ClientInstance.ReadLastRecord);
			
			WriteOutputCS.writeInt(CHinBytes.length);
			WriteOutputCS.write(CHinBytes);
			WriteOutputCS.flush();
			
			int ChunkSize =  ReadInputCS.readInt();
			ChunkSize -= ChunkServer.PayloadSZ;  //reduce the length by the first four bytes that identify the length
			
			if(ChunkSize == 0) {
				return FSReturnVals.RecDoesNotExist;
			}
			
			payload = Client.RecvPayload("Client", ReadInputCS, ChunkSize); 
			int index = ReadInputCS.readInt();
			
			RecordID = new RID();
			RecordID.ChunkHandle = ChunkHandle;
			RecordID.index = index;
			
			return FSReturnVals.Success;
			
			
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
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, byte[] payload, RID RecordID) {
		if(ofh.ChunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		if(pivot.index < 0) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		String ChunkHandle = pivot.ChunkHandle;
		byte[] CHinBytes = ChunkHandle.getBytes();
		Location primaryLoc = ofh.getPrimaryLocation();
		
		try {
			Socket CSConnection = new Socket(primaryLoc.IPAddress, primaryLoc.port);
			ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
			ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
			
			//WriteOutputCS.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutputCS.writeInt(ClientInstance.ReadLastRecord);
			
			WriteOutputCS.writeInt(CHinBytes.length);
			WriteOutputCS.write(CHinBytes);
			WriteOutputCS.flush();
			
			int ChunkSize =  ReadInputCS.readInt();
			ChunkSize -= ChunkServer.PayloadSZ;  //reduce the length by the first four bytes that identify the length
			
			if(ChunkSize == 0) {
				return FSReturnVals.RecDoesNotExist;
			}
			
			payload = Client.RecvPayload("Client", ReadInputCS, ChunkSize); 
			int index = ReadInputCS.readInt();
			
			RecordID = new RID();
			RecordID.ChunkHandle = ChunkHandle;
			RecordID.index = index;
			
			return FSReturnVals.Success;
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return FSReturnVals.Fail;
		}
		
		return null;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, rec, recn) 2. ReadPrevRecord(FH1,
	 * recn-1, rec, rec2) 3. ReadPrevRecord(FH1, recn-2, rec, rec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, byte[] payload, RID RecordID) {
		return null;
	}


	
}
