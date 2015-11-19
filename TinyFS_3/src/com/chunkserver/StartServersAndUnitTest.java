package com.chunkserver;

import UnitTests3.UnitTest5;
import UnitTests3.UnitTest6;
import UnitTests3.UnitTest4;
import master.TFSMaster;

public class StartServersAndUnitTest
{

	public static void main(String [] args)
	{
		
		ChunkServer cs = new ChunkServer();
		cs.start();
		
		try
		{
			UnitTest4.main(args);
			//UnitTest5.main(args);
			//UnitTest6.main(args);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
