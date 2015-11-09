package com.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class FileHandle {
	
	String fileName;
	String[] chunkHandles; //array of all chunk handles that constitute the file
	HashMap<String, String> chunksToLocations;
	
	public FileHandle(){}
	
	public void setFileName(String name)
	{
		this.fileName = name;
	}
	public void setHandles(String[] handles)
	{
		this.chunkHandles = handles;
	}
	public void setLocations(HashMap<String,String> chunksAndLocations)
	{
		this.chunksToLocations = chunksAndLocations;
	}
	
	
}
