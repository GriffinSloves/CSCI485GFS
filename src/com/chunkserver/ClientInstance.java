package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Vector;

import com.client.Client;
import com.client.RID;

import master.Location;

public class ClientInstance extends Thread
{
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	public static final int getChunks = 104;
	public static final int AppendRecord = 105;
	public static final int DeleteRecord = 106;
	public static final int ReadFirstRecord = 107;
	public static final int ReadLastRecord = 108;
	public static final int ReadNextRecord = 109;
	public static final int ReadPreviousRecord = 110;
	public static final int CloseSockets = 111;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	private ChunkServer cs;
	private Socket ClientConnection;
	private ObjectOutputStream WriteOutput;
	private ObjectInputStream ReadInput;
	
	public ClientInstance(ChunkServer cs, Socket s)
	{
		this.cs = cs;
		this.ClientConnection = s;
	}
	
	public void run()
	{
		try {
			ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
			WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
			int offset;
			int payloadlength;
			int chunkhandlesize;
			byte[] CHinBytes;
			byte[] payload;
			RID rid;
			String ChunkHandle;
			int CMD = 0;
			Location currLoc;
			Vector<Location> Locations;
			int success;
			Vector<Location> failedLocs = new Vector<Location>();
			//Use the existing input and output stream as long as the client is connected
			while (!ClientConnection.isClosed() && CMD != CloseSockets) {
				CMD = ChunkServer.ReadIntFromInputStream("ClientInstance0", ReadInput);
				
				switch (CMD){
				case CreateChunkCMD:
					ChunkHandle = cs.createChunk();
					CHinBytes = ChunkHandle.getBytes();
					try
					{
						currLoc = (Location)ReadInput.readObject();
						Locations = (Vector<Location>)ReadInput.readObject();
						for(int i = 0; i < Locations.size(); i++)
						{
							Location nextLoc = Locations.elementAt(i);
							if(!nextLoc.equals(currLoc))
							{
								Socket CSConnection = new Socket(nextLoc.IPAddress, nextLoc.port);
								ObjectOutputStream WriteOutputCS = new ObjectOutputStream(CSConnection.getOutputStream());
								WriteOutputCS.flush();
								ObjectInputStream ReadInputCS = new ObjectInputStream(CSConnection.getInputStream());
								WriteOutputCS.writeInt(ChunkServer.CreateChunkCMD);
								WriteOutputCS.flush();
								success = ReadInputCS.readInt();
								if(success != 1)
								{
									failedLocs.addElement(nextLoc);
								}
							}
							
						}
						WriteOutput.writeInt(CHinBytes.length);
						WriteOutput.write(CHinBytes);
						WriteOutput.flush();
					}
					catch(ClassNotFoundException cnfe)
					{
						cnfe.printStackTrace();
						WriteOutput.writeInt(-1);
					}
					break;

				case WriteChunkCMD:
					
					payloadlength =  ChunkServer.ReadIntFromInputStream("ClientInstance3", ReadInput);
					payload = ChunkServer.RecvPayload("ChunkServer", ReadInput, payloadlength);
					chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance3", ReadInput);
					if (chunkhandlesize < 0)
						System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
					CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
					ChunkHandle = (new String(CHinBytes)).toString();
			
					//Call the writeChunk command
					WriteOutput.writeInt(cs.append(ChunkHandle, payload));
					WriteOutput.flush();
					break;
				case DeleteRecord:
					int recordIndex = ChunkServer.ReadIntFromInputStream("ClientInstance4", ReadInput);		
					chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance4", ReadInput);
					CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
					ChunkHandle = (new String(CHinBytes)).toString();					
					if(cs.deleteRecord(ChunkHandle, recordIndex)) {
						WriteOutput.writeInt(ChunkServer.TRUE); 
					} else {
						WriteOutput.writeInt(ChunkServer.FALSE);
					}
					
					WriteOutput.flush();
					break;
					
				case ReadFirstRecord:
					chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance5", ReadInput);
					CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
					ChunkHandle = (new String(CHinBytes)).toString();
					rid = new RID(ChunkHandle, 0);
					//If read record failed, rid.index will be -1
					payload = cs.readRecord(rid, true);
					if (payload == null)
						WriteOutput.writeInt(-1);
					else {
						WriteOutput.writeInt(rid.index);
						WriteOutput.writeInt(payload.length);
						WriteOutput.write(payload);
					}
					WriteOutput.flush();
					break;
					
				case ReadLastRecord:
					chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance6", ReadInput);
					CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
					ChunkHandle = (new String(CHinBytes)).toString();
					int index = cs.getLastIndex(ChunkHandle);
					rid = new RID(ChunkHandle, index);
					payload = cs.readRecord(rid, false);
					
					if (payload == null)
						WriteOutput.writeInt(-1);
					else {
						WriteOutput.writeInt(rid.index);
						WriteOutput.writeInt(payload.length);
						WriteOutput.write(payload);
					}
					
					WriteOutput.writeInt(cs.getLastIndex(ChunkHandle));
					WriteOutput.flush();
					break;
				
				case ReadNextRecord:
					int nextIndex = ChunkServer.ReadIntFromInputStream("ClientInstance7", ReadInput);
					chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance7", ReadInput);
					CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
					ChunkHandle = (new String(CHinBytes)).toString();
										
					rid = new RID(ChunkHandle, nextIndex);
					payload = cs.readRecord(rid, true);
					
					if(payload == null) {
						WriteOutput.writeInt(-1);
					} else {
						WriteOutput.writeInt(rid.index);
						WriteOutput.writeInt(payload.length);
						WriteOutput.write(payload);
					}

					WriteOutput.flush();
					break;
					
				case ReadPreviousRecord:
					index = ChunkServer.ReadIntFromInputStream("ClientInstance8", ReadInput);
					chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance8", ReadInput);
					CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
					ChunkHandle = (new String(CHinBytes)).toString();
					if(index == -1)
					{
						index = cs.getLastIndex(ChunkHandle);
					}
					rid = new RID(ChunkHandle, index);
					payload = cs.readRecord(rid, false);
					
					if(payload == null) {
						WriteOutput.writeInt(-1);
					} else {
						WriteOutput.writeInt(rid.index);
						WriteOutput.writeInt(payload.length);
						WriteOutput.write(payload);
					}

					WriteOutput.flush();
					break;				
				case CloseSockets:
					break;
				default:
					System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
					break;
				}
			}
		} catch (IOException ex){
			System.out.println("Client Disconnected");
		} finally {
			try {
				if (ClientConnection != null && !ClientConnection.isClosed())
					ClientConnection.close();
				if (ReadInput != null)
					ReadInput.close();
				if (WriteOutput != null) WriteOutput.close();
			} catch (IOException fex){
				System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
				fex.printStackTrace();
			}
		}
	}
}