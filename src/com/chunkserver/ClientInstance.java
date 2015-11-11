package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import com.client.Client;

public class ClientInstance extends Thread
{
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	public static final int getChunks = 104;
	public static final int AppendRecord = 105;
	public static final int DeleteRecord = 106;
	public static final int ReadRecord = 107;
	
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
		boolean done = false;
		while (!done){
			try {
				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				
				//Use the existing input and output stream as long as the client is connected
				while (!ClientConnection.isClosed()) {
					int payloadsize =  ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
					if (payloadsize == -1) 
						break;
					int CMD = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
					switch (CMD){
					case CreateChunkCMD:
						String chunkhandle = cs.createChunk();
						byte[] CHinbytes = chunkhandle.getBytes();
						WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
						WriteOutput.write(CHinbytes);
						WriteOutput.flush();
						break;

					case ReadChunkCMD:
						int offset =  ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						int payloadlength =  ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
						byte[] CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						String ChunkHandle = (new String(CHinBytes)).toString();
						
						byte[] res = cs.readChunk(ChunkHandle, offset, payloadlength);
						
						if (res == null)
							WriteOutput.writeInt(ChunkServer.PayloadSZ);
						else {
							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
							WriteOutput.write(res);
						}
						WriteOutput.flush();
						break;

					case WriteChunkCMD:
						offset =  ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						payloadlength =  ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						byte[] payload = ChunkServer.RecvPayload("ChunkServer", ReadInput, payloadlength);
						chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
						CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();
						
						//Call the writeChunk command
						if (cs.writeChunk(ChunkHandle, payload, offset)) {
							WriteOutput.writeInt(offset);
							WriteOutput.writeInt(ChunkServer.TRUE);
						}	
						else {
							WriteOutput.writeInt(offset);
							WriteOutput.writeInt(ChunkServer.FALSE);
						}
						
						WriteOutput.flush();
						break;
					case DeleteRecord:
						offset = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						int recordSize = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);		
						chunkhandlesize = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();
						
						if(cs.deleteRecord(ChunkHandle, offset, recordSize)) {
							WriteOutput.writeInt(ChunkServer.TRUE); 
						} else {
							WriteOutput.writeInt(ChunkServer.FALSE);
						}
						
						WriteOutput.flush();
						break;
					case ReadRecord:
						int first_or_last = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						chunkhandlesize = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
						CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();
						
						res = cs.readRecord(ChunkHandle, first_or_last);
						
						if (res == null)
							WriteOutput.writeInt(ChunkServer.PayloadSZ);
						else {
							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
							WriteOutput.write(res);
						}
						WriteOutput.flush();
						
					default:
						System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
						break;
					}
				}
			} catch (IOException ex){
				System.out.println("Client Disconnected");
			} finally {
				try {
					if (ClientConnection != null)
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
}