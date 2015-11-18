package com.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import master.Location;

public class FileHandle {
	
	String filePath;
	Vector<String> ChunkHandles; //array of all chunk handles that constitute the file
	HashMap<String, Vector<Location>> chunksToLocations;
	Location primaryLoc;
	
	public FileHandle(){}
	
	public void setFilePath(String path)
	{
		this.filePath = path;
	}

	public String getFilePath()
	{
		return this.filePath;
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
	
	// Right now it is assuming 
	public Location getNextLocation(String chunk) {
		Vector<Location> locations = chunksToLocations.get(chunk);
		int curIndex = -1;
		
		for(int i = 0; i < locations.size(); i++) {
			if(primaryLoc.equals(locations.get(i))) {
				curIndex = i+1;
				break;
			}
		}
		
		if(curIndex >= locations.size()) {
			return null;
		}
		primaryLoc = locations.get(curIndex);
		return primaryLoc;
	}
	
}
