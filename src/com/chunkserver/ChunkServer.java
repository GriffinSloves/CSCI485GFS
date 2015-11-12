package com.chunkserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.chunkserver.ClientInstance;
import com.client.Client;
import com.client.RID;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer extends Thread implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ClientConfigFile = "ClientConfig.txt";
	
	//Used for the file system
	public static long counter;
	
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	public static final int getChunks = 104;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	
	public static final String MasterIPAddress = "127.0.0.1";
	public static final int MasterPort = 6789;
	
	private HashMap<String, Lease> LeaseMap;
	private HashMap<String, String[]> ChunkReplicaMap;
	
	private ServerSocket ss;
	private Socket MasterConnection;
	private ObjectInputStream ReadInput;
	private ObjectOutputStream WriteOutput;
	/**
	 * Initialize the chunk server
	 */
	
	//ChunkServer acts as Server for CS to Client connection
	//ChunkServer acts as Client in CS to Master connection
	public ChunkServer(){
		File dir = new File(filePath);
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
		try
		{
			ss = new ServerSocket(0);
			int port = ss.getLocalPort();
			MasterConnection = new Socket(MasterIPAddress, MasterPort);
			WriteOutput = new ObjectOutputStream(MasterConnection.getOutputStream());
			ReadInput = new ObjectInputStream(MasterConnection.getInputStream());
			WriteOutput.writeObject(ss.getInetAddress().getHostAddress());
			WriteOutput.writeInt(port);
			WriteOutput.flush();
		}
		catch(IOException ex)
		{
			System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
		}
		finally {
			try {
				if (MasterConnection != null)
					MasterConnection.close();
				if (ReadInput != null)
					ReadInput.close();
				if (WriteOutput != null) WriteOutput.close();
			} catch (IOException fex){
				System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
				fex.printStackTrace();
			}
		}
		
		//Write to Master all of the chunkhandles being stored on this chunkserver
		//Every second, check which leases are expiring and renew the necessary ones
		RenewLeaseThread rlt = new RenewLeaseThread(this);
		rlt.start();
	}
	
	
	public void run()
	{
		try
		{
			
			while(true)
			{
				Socket s = ss.accept(); //Blocking
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ClientInstance ci = new ClientInstance(this, s);
				ci.start();
			}
			
			
		}
		catch (IOException ioe)
		{
			ioe.printStackTrace();
		}
		finally
		{
			try
			{
				if(ss != null)
				{
					ss.close();
				}
			
			}
			catch(IOException ioe)
			{
				ioe.printStackTrace();
			}
		}
	}
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	public int append(String ChunkHandle, byte[] payload) {
		try {
			
			File file = new File(filePath + ChunkHandle);
			RandomAccessFile raf;
			byte [] intBuf = new byte[4];
			if(!file.exists())
			{
				raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
				//Write Header
				//0-3, num records
				//4-7, Start of next record
				//8-11, End of free space
				intBuf = ChunkServer.convertIntToBytes(0);
				raf.write(intBuf, 0, 4);
				intBuf = ChunkServer.convertIntToBytes(12);
				raf.write(intBuf, 4, 4);
				intBuf = ChunkServer.convertIntToBytes(4096);
				raf.write(intBuf, 8, 4);
				
			}
			else
			{
				raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			}
			
			raf.read(intBuf, 0, 4);
			int numRecords = ChunkServer.convertBytesToInt(intBuf);
			raf.read(intBuf, 4, 4);
			int offset = ChunkServer.convertBytesToInt(intBuf);
			raf.read(intBuf, 8, 4);
			int endSpace = ChunkServer.convertBytesToInt(intBuf);
			int size = payload.length;
			if(offset + size + 8 > endSpace) //Too big of a payload; 8 because we need to write size of payload, and offset. 
			{
				raf.close();
				return -1;
			}
			intBuf = ChunkServer.convertIntToBytes(size);
			//Write record size
			if(!writeChunk(ChunkHandle, intBuf, offset))
			{
				raf.close();
				return -1;
			}
			//Write record
			if(!writeChunk(ChunkHandle, payload, offset + 4))
			{
				raf.close();
				return -1;
			}
			//Write metadata
			//Write offset of current record
			endSpace -= 4;
			intBuf = ChunkServer.convertIntToBytes(offset);
			if(!writeChunk(ChunkHandle, intBuf, endSpace))
			{
				raf.close();
				return -1;
			}
			//Write numRecords
			numRecords++;
			intBuf = ChunkServer.convertIntToBytes(numRecords);
			if(!writeChunk(ChunkHandle, intBuf, 0))
			{
				raf.close();
				return -1;
			}
			//Write start of next record
			offset = offset + size + 4;
			intBuf = ChunkServer.convertIntToBytes(offset);
			if(!writeChunk(ChunkHandle, intBuf, 4))
			{
				raf.close();
				return -1;
			}
			//Write end of free space
			intBuf = ChunkServer.convertIntToBytes(endSpace);
			if(!writeChunk(ChunkHandle, intBuf, 8))
			{
				raf.close();
				return -1;
			}
			
			raf.close();
			return numRecords;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}	
	}
	
	public boolean deleteRecord(String ChunkHandle, int index) {
		try {
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			byte [] intBuf = new byte[4];
			raf.read(intBuf, 8, 4);
			int endSpace = ChunkServer.convertBytesToInt(intBuf);
			int offset = 4096 - (index + 1) * 4;
			if(endSpace > offset)
			{
				raf.close();
				return false;
			}
			intBuf = ChunkServer.convertIntToBytes(-1);
			if(!writeChunk(ChunkHandle, intBuf, offset))
			{
				raf.close();
				return false;
			}
			
			raf.close();
			
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}
	
	public byte[] readRecord(RID rid, boolean forward) 
	{
		String ChunkHandle = rid.ChunkHandle;
		int index = rid.index;
		byte [] payload = null;
		byte [] intBuf = new byte[4];
		boolean foundRecord = false;
		try
		{
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			int indexOffset;
			int offset = 0;
			raf.read(intBuf, 8, 4);
			int endSpace = ChunkServer.convertBytesToInt(intBuf);
			while(!foundRecord)
			{
				indexOffset = 4096 - (index + 1) * 4;
				//Error checking, making sure we're always checking a valid record
				if(endSpace < indexOffset)
				{
					rid.index = -1;
					return null;
				}
				if(index < 0)
				{
					rid.index = -1;
					return null;
				}
				raf.read(intBuf, indexOffset, 4);
				offset = ChunkServer.convertBytesToInt(intBuf);
				//If deleted, move to the next record
				if(offset == -1)
				{
					if(forward)
					{
						index++;
					}
					else
					{
						index--;
					}
				}
				else
				{
					foundRecord = true;
				}
			}
			raf.read(intBuf, offset, 4);
			int size = ChunkServer.convertBytesToInt(intBuf);
			payload = readChunk(ChunkHandle, offset + 4, size);
			raf.close();
			return payload;
		}
		catch (IOException e) {
			return null;
		}
		
		
	}
	
	public int getLastIndex(String ChunkHandle)
	{
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			byte [] intBuf = new byte[4];
			raf.read(intBuf, 8, 4);
			int endSpace = ChunkServer.convertBytesToInt(intBuf);
			int index = (4096 - endSpace - 4) / 4;
			
			return index;
			
		} catch (IOException ex) {
			ex.printStackTrace();
			return -1;
		}
	}
	

	
/*	public synchronized void ObtainLease(String ChunkHandle)
	{
		//Ask master to obtain lease on ChunkHandle
		byte [] payload = ChunkHandle.getBytes();
		try
		{
			int code = 202;
			WriteOutput.writeInt(code);
			WriteOutput.writeInt(payload.length);
			WriteOutput.write(payload);
			WriteOutput.flush();
			
			int retVal = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
			if(retVal == TRUE)
			{
				Lease lease = new Lease(ChunkHandle);
				LeaseMap.put(ChunkHandle, lease);
			}
					
		}
		catch(IOException ex)
		{
			System.out.println("Error (ChunkServer):  IOException in ObtainLease.");
		}
		finally {
			try {
				if (MasterConnection != null)
					MasterConnection.close();
				if (ReadInput != null)
					ReadInput.close();
				if (WriteOutput != null) WriteOutput.close();
			} catch (IOException fex){
				System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
				fex.printStackTrace();
			}
		}
		return;
	}*/
	

	public synchronized void RenewLease(Lease lease)
	{
		//Ask master for lease renewal
		String ChunkHandle = lease.getChunkHandle();
		byte [] payload = ChunkHandle.getBytes();
		try
		{
			int code = 202;
			WriteOutput.writeInt(code);
			WriteOutput.writeInt(payload.length);
			WriteOutput.write(payload);
			WriteOutput.flush();
			
			int retVal = ChunkServer.ReadIntFromInputStream("ChunkServer", ReadInput);
			if(retVal == TRUE)
			{
				lease.updateLeaseCS();
			}
			else
			{
				LeaseMap.remove(ChunkHandle);
			}
			
		}
		catch(IOException ex)
		{
			System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
		}
		finally {
			try {
				if (MasterConnection != null)
					MasterConnection.close();
				if (ReadInput != null)
					ReadInput.close();
				if (WriteOutput != null) WriteOutput.close();
			} catch (IOException fex){
				System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
				fex.printStackTrace();
			}
		}
		
	}
	
	public synchronized void sendHeartbeatMessage()
	{
		try
		{
			int code = 201;
			WriteOutput.writeInt(code); 
			//send IPAddress of CS to identify which chunks are on each CS
			String IPAddressOfThisCS = InetAddress.getLocalHost().getHostAddress();
			byte[] IPasByte = IPAddressOfThisCS.getBytes();
			WriteOutput.write(IPasByte);
			WriteOutput.flush();
			//send chunk list
			String[] chunksOnThisCS = ChunkServer.listChunks();
			WriteOutput.writeObject(chunksOnThisCS);
			WriteOutput.flush();
			
			String [] ChunkHandleList = (String [])ReadInput.readObject();
			deleteFiles(ChunkHandleList);
		}
		catch(IOException ex)
		{
			System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
		}
		catch(ClassNotFoundException cnfe)
		{
			System.out.println("cnfe in ChunkServer.sendHeartbeatMessage(): " + cnfe.getMessage());
		}
		finally {
			try {
				if (MasterConnection != null)
					MasterConnection.close();
				if (ReadInput != null)
					ReadInput.close();
				if (WriteOutput != null) WriteOutput.close();
			} catch (IOException fex){
				System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
				fex.printStackTrace();
			}
		}
		
	}
	
	public void deleteFiles(String [] ChunkHandleList)
	{
		for(int i = 0; i < ChunkHandleList.length; i++)
		{
			File file = new File(filePath + ChunkHandleList[i]);
			file.delete();
		}
	}
	
	public static int convertBytesToInt(byte [] byteArray)
	{
		return ByteBuffer.wrap(byteArray).getInt();
	}
	
	public static byte [] convertIntToBytes(int toBytes)
	{
		return ByteBuffer.allocate(4).putInt(toBytes).array();
	}
	
	public static String[] listChunks()
	{
		
		String[] chunkFiles = null;
		File chunkDir = new File(filePath);
		chunkFiles = chunkDir.list();
		
		return chunkFiles;
	}
	
	
	public static byte[] RecvPayload(String caller, ObjectInputStream instream, int sz){
		byte[] tmpbuf = new byte[sz];
		byte[] InputBuff = new byte[sz];
		int ReadBytes = 0;
		while (ReadBytes != sz){
			int cntr=-1;
			try {
				cntr = instream.read( tmpbuf, 0, (sz-ReadBytes) );
				for (int j=0; j < cntr; j++){
					InputBuff[ReadBytes+j]=tmpbuf[j];
				}
			} catch (IOException e) {
				System.out.println("Error in RecvPayload ("+caller+"), failed to read "+sz+" after reading "+ReadBytes+" bytes.");
				return null;
			}
			if (cntr == -1) {
				System.out.println("Error in RecvPayload ("+caller+"), failed to read "+sz+" bytes.");
				return null;
			}
			else ReadBytes += cntr;
		}
		return InputBuff;
	}
	
	public static int ReadIntFromInputStream(String caller, ObjectInputStream instream){
		int PayloadSize = -1;
		
		byte[] InputBuff = RecvPayload(caller, instream, 4);
		if (InputBuff != null)
			PayloadSize = ByteBuffer.wrap(InputBuff).getInt();
		return PayloadSize;
	}
	
	public class MasterThread extends Thread
	{
		
		private ChunkServer cs;
		private Socket MasterConnection;
		private ObjectOutputStream WriteOutput;
		private ObjectInputStream ReadInput;
		public MasterThread(ChunkServer cs)
		{
			this.cs = cs;
		}
		
		public void run()
		{
			
		}
	}
	
	public class RenewLeaseThread extends Thread
	{
		
		public ChunkServer cs;
		
		public RenewLeaseThread(ChunkServer cs)
		{
			this.cs = cs;

		}
		
		public void run()
		{
			//Every second, check each of the currently held leases, and attempt to renew them if time
			while(true)
			{
				
				try
				{
					Lease lease;
					Iterator<Entry<String, Lease>> it = cs.LeaseMap.entrySet().iterator();
				    while (it.hasNext()) {
				        HashMap.Entry<String, Lease> pair = (HashMap.Entry<String, Lease>)it.next();
				        lease = (Lease)pair.getValue();
				        if(lease.needsRenewal())
				        {
				        	cs.RenewLease(lease);
				        }
				    }
				    cs.sendHeartbeatMessage();
					Thread.sleep(1000);
				}
				catch(InterruptedException ie)
				{
					
				}
				
			}
		}
	}
	
}
