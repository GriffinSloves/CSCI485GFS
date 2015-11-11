package com.client;

public class RID {
	
	int offset;
	String chunkhandle;
	int recordSize;
	
	public RID(){}
	
	public int getOffset()
	{
		return this.offset;
	}
	public void setOffset(int offset)
	{
		this.offset = offset;
	}
	
	public String getchunkhandle()
	{
		return this.chunkhandle;
	}
	public void setChunkHandle(String chunkhandle)
	{
		this.chunkhandle  =chunkhandle;
	}
	public void setSize(int size)
	{
		recordSize = size;
	}
	
	public int getSize()
	{
		return recordSize;
	}
}
