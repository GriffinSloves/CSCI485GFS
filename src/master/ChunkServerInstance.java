package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Vector;

import com.client.Client;

public class ChunkServerInstance extends Thread
{

	private TFSMaster master;
	private Socket CSConnection;
	private ObjectOutputStream WriteOutput;
	private ObjectInputStream ReadInput;
	
	private String IPAddress = null;
	private int port;
	
	public ChunkServerInstance(TFSMaster master, Socket CSConnection)
	{
		this.master = master;
		this.CSConnection = CSConnection;
		try
		{
			WriteOutput = new ObjectOutputStream(CSConnection.getOutputStream());
			ReadInput = new ObjectInputStream(CSConnection.getInputStream());
			IPAddress = CSConnection.getInetAddress().getHostAddress();
			port = CSConnection.getPort();
		}
		catch (IOException ex){
			System.out.println("Client Disconnected");
		} finally {
			try {
				if (CSConnection != null)
					CSConnection.close();
				if (ReadInput != null)
					ReadInput.close();
				if (WriteOutput != null) WriteOutput.close();
			} catch (IOException fex){
				System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
				fex.printStackTrace();
			}
		}
	}
	
	public void run()
	{
		try
		{
			int code;
			int size;
			String ChunkHandle;
			String [] ChunkHandles;
			byte [] byteArray;
			Vector<String> DeletedChunks;
			while(true)
			{
				code = ReadInput.readInt();
				switch(code){
				case 201: //Heartbeat message
					ChunkHandles = (String [])ReadInput.readObject();
					DeletedChunks = master.updateChunkLocations(IPAddress, ChunkHandles);
					WriteOutput.writeObject(DeletedChunks);
					WriteOutput.flush();
					break;
				case 202: //Renew Lease
					size = Client.ReadIntFromInputStream("Master", ReadInput);
					byteArray = Client.RecvPayload("Master", ReadInput, size);
					ChunkHandle = new String(byteArray);
					master.renewLease(ChunkHandle);
					break;
				}
			}
		}
		catch(ClassNotFoundException cnfe)
		{
			System.out.println(cnfe.getMessage());
		}
		catch (IOException ex){
			System.out.println("Client Disconnected");
		} finally {
			try {
				if (CSConnection != null)
					CSConnection.close();
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