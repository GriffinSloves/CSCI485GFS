package master;

public class Location {

	public String IPAddress;
	public int port;
	public Location(String IPAddress, int port)
	{
		this.IPAddress = IPAddress;
		this.port = port;
	}
	
	public boolean equals(Location other)
	{
		return (other.IPAddress == this.IPAddress && other.port == this.port);
	}
}
