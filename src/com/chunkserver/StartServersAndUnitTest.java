package com.chunkserver;

import UnitTests3.UnitTest4;
import master.TFSMaster;

public class StartServersAndUnitTest
{

	public static void main(String [] args)
	{
		
		ChunkServer cs = new ChunkServer();
		cs.start();
		UnitTest4.main(args);
	}
}
