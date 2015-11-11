package com.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.chunkserver.ClientInstance;
import com.client.Client;
import master.TFSMaster;

//TODO connect sockets during each operation depending on where the chunkserver is located
// Can't connect in instructor because we do not know the location that we want to connect to

public class ClientRec {
	static int ServerPort = 0;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	
	public ClientRec() { //Will have to change. 
						 //The client will connect to different chunk servers depending on which one as the data
		if (ClientSocket != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(ChunkServer.ClientConfigFile));
			String port = binput.readLine();
			port = port.substring( port.indexOf(':')+1 );
			ServerPort = Integer.parseInt(port);
			
			ClientSocket = new Socket("127.0.0.1", ServerPort); //should client be reading from config?
			WriteOutput = new ObjectOutputStream(ClientSocket.getOutputStream());
			ReadInput = new ObjectInputStream(ClientSocket.getInputStream());
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
		String ChunkHandle = ofh.chunkHandles.get(ofh.chunkHandles.size()-1);
		byte[] CHinBytes = ChunkHandle.getBytes();
		try {
			WriteOutput.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + payload.length + CHinBytes.length);
			WriteOutput.writeInt(ChunkServer.WriteChunkCMD);
			WriteOutput.writeInt(-1); //Specifies to WriteChunk to append the data
			WriteOutput.writeInt(payload.length);
			WriteOutput.write(payload);
			WriteOutput.write(CHinBytes);
			WriteOutput.flush();
			
			int offset = ReadInput.readInt();
		/*	if(offset == -1) {
				RecordID = null;
				return FSReturnVals.Fail;
			}*/
			
			RecordID.chunkhandle = ChunkHandle;
			RecordID.offset = offset;
			RecordID.recordSize = payload.length;
			
			return FSReturnVals.Success;
			
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
		if(RecordID.isDeleted) {
			return FSReturnVals.RecDoesNotExist;
		}
		
		String ChunkHandle = ofh.chunkHandles.get(ofh.chunkHandles.size()-1);
		byte[] CHinBytes = ChunkHandle.getBytes();
		
		try {
			WriteOutput.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutput.writeInt(ClientInstance.DeleteRecord);
			WriteOutput.writeInt(RecordID.offset);
			WriteOutput.writeInt(RecordID.recordSize);
			WriteOutput.writeInt(CHinBytes.length);
			WriteOutput.write(CHinBytes);
			WriteOutput.flush();
			
			int done = ReadInput.readInt();
			if(done == ChunkServer.FALSE) {
				return FSReturnVals.Fail;
			} else {
				RecordID.isDeleted = true;
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
		
		if(ofh.chunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		
		String ChunkHandle = ofh.chunkHandles.get(0);
		byte[] CHinBytes = ChunkHandle.getBytes();
		
		try {
			WriteOutput.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutput.writeInt(ClientInstance.ReadRecord);
			
			WriteOutput.writeInt(0);
			WriteOutput.writeInt(CHinBytes.length);
			WriteOutput.write(CHinBytes);
			WriteOutput.flush();
			
			int ChunkSize =  Client.ReadIntFromInputStream("ClientRec", ReadInput);
			ChunkSize -= ChunkServer.PayloadSZ;  //reduce the length by the first four bytes that identify the length
			payload = Client.RecvPayload("Client", ReadInput, ChunkSize); 
			
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
		if(ofh.chunkHandles.size() == 0) {
			return FSReturnVals.BadHandle;
		}
		
		String ChunkHandle = ofh.chunkHandles.get(0);
		byte[] CHinBytes = ChunkHandle.getBytes();
		
		try {
			WriteOutput.writeInt(ChunkServer.PayloadSZ + ChunkServer.CMDlength + (2*4) + CHinBytes.length);
			WriteOutput.writeInt(ClientInstance.ReadRecord);
			
			WriteOutput.writeInt(1);
			WriteOutput.writeInt(CHinBytes.length);
			WriteOutput.write(CHinBytes);
			WriteOutput.flush();
			
			int ChunkSize =  Client.ReadIntFromInputStream("ClientRec", ReadInput);
			ChunkSize -= ChunkServer.PayloadSZ;  //reduce the length by the first four bytes that identify the length
			payload = Client.RecvPayload("Client", ReadInput, ChunkSize); 
			
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
