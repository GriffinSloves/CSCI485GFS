package UnitTests3;

import java.util.ArrayList;
import java.util.Arrays;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;

/**
 * UnitTest2 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest2 {

	public static void main(String[] args) {
		test2(new ClientFS());
	}
	
	public static void test2(ClientFS cfs){
		UnitTest1 ut1 = new UnitTest1();
		ut1.test1(cfs);
		int N = ut1.N;
		String dir1 = "Shahram";
		FSReturnVals fsrv = cfs.DeleteDir("/" + dir1 + "/", String.valueOf(N));
		//String[] ret1 = cfs.ListDir(dir1);UNIT TEST 2 Changed see Jason's 
		String[] ret1 = cfs.ListDir("/" + dir1);
		
		boolean isExist = isDirExist(ret1, "/" + dir1+"/"+N);
		if(isExist == true){
			System.out.println("Unit test 2 result: fail! - 1");
    		return;
		}
		//System.out.println("Got Thru UT1");
		
		String dir2 = "Ghandeharizadeh";
		String lastSec = "/" + dir2;
		for(int i = 1; i < N; i++){
			lastSec = lastSec + "/" + i;
		}
		fsrv = cfs.DeleteDir(lastSec + "/", String.valueOf(N));
		
		String[] ret2 = cfs.ListDir(lastSec);
		System.out.println(ret2 == null);
		isExist = isDirExist(ret2, lastSec + "/" + N);
		if(isExist == true){
			System.out.println("Unit test 2 result: fail! - 2");
    		return;
		}
		System.out.println("Got Thru UT2");
		
		fsrv = cfs.DeleteDir("/", dir1);
		if(fsrv == FSReturnVals.DirNotEmpty){
			System.out.println("Good!  Detected " + dir1 + " exists.");
		} else {
			System.out.println("Unit test 2 result: fail! - 3");
    		return;
		}
		
		System.out.println("Got Thru UT3");
		
		fsrv = cfs.DeleteDir("/" + dir2 + "/1/", "2");
		if(fsrv == FSReturnVals.DirNotEmpty){
			System.out.println("Good!  Detected /" + dir2 + "/1/2 exists.");
		} else {
			System.out.println("Unit test 2 result: fail! - 4");
    		return;
		}
		
		System.out.println("Got Thru UT4");
		
		for(int i = 1; i < N; i++){
			fsrv = cfs.RenameDir("/" + dir1 + "/" + i, "/" + dir1 + "/" + i + "i");
			if( fsrv != FSReturnVals.Success){
				System.out.println("Unit test 2 result: fail! - 5");
	    		return;
			}
		}
		
		fsrv = cfs.RenameDir("/" + dir2, "/ShahramGhandeharizadeh");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 2 result: fail! - 6");
    		return;
		}
		System.out.println("Unit test 2 result: success!");
	}
	
	public static boolean isDirExist(String[] arr, String token){
		for (int i=0; i < arr.length; i++)
			if (arr[i].equals(token)) return true;
		return false;
	}

}
