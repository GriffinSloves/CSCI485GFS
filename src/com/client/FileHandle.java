package com.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import master.Location;

public class FileHandle {
	
	String fileName;
	Vector<String> ChunkHandles; //array of all chunk handles that constitute the file
	HashMap<String, Vector<Location>> chunksToLocations;
	Location primaryLoc;
	
	public FileHandle(){}
	
	public void setFileName(String name)
	{
		this.fileName = name;
	}
	
	public String getFileName()
	{
		return fileName;
	}
	public void setHandles(Vector<String> chunksOfFile)
	{
		this.ChunkHandles = chunksOfFile;
	}
	
	public Vector<String> getChunkHandles()
	{
		return ChunkHandles;
	}
	
	public void setLocations(HashMap<String, Vector<Location>> locationsOfChunks)
	{
		this.chunksToLocations = locationsOfChunks;
	}
	
	public HashMap<String, Vector<Location>> getLocations()
	{
		return chunksToLocations;
	}
	
	public void setPrimaryLocation(Location loc)
	{
		primaryLoc = loc;
	}
	
	public Location getPrimaryLocation()
	{
		return primaryLoc;
	}
	
}
