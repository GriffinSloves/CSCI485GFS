package com.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class FileHandle {
	
	String fileName;
	Vector<String> chunkHandles; //array of all chunk handles that constitute the file
	HashMap<String, Vector<String>> chunksToLocations;
	
	public FileHandle(){}
	
	public void setFileName(String name)
	{
		this.fileName = name;
	}
	public String getFileName(){
		return this.fileName;
	}
	public void setHandles(Vector<String> chunksOfFile)
	{
		this.chunkHandles = chunksOfFile;
	}
	public void setLocations(HashMap<String, Vector<String>> locationsOfChunks)
	{
		this.chunksToLocations = locationsOfChunks;
	}
	
	
}
