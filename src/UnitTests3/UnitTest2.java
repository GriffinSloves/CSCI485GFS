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
		
		cfs.displayNamespace();
		
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
		
		
		String dir2 = "Ghandeharizadeh";
		String lastSec = "/" + dir2;
		for(int i = 1; i < N; i++){
			lastSec = lastSec + "/" + i;
		}
		fsrv = cfs.DeleteDir(lastSec + "/", String.valueOf(N));
		
		String[] ret2 = cfs.ListDir(lastSec);
		isExist = isDirExist(ret2, lastSec + "/" + N);
		if(isExist == true){
			System.out.println("Unit test 2 result: fail! - 2");
    		return;
		}
		
		
		fsrv = cfs.DeleteDir("/", dir1);
		if(fsrv == FSReturnVals.DirNotEmpty){
			System.out.println("Good!  Detected " + dir1 + " exists.");
		} else {
			System.out.println("Unit test 2 result: fail! - 3");
    		return;
		}
		
		
		
		fsrv = cfs.DeleteDir("/" + dir2 + "/1/", "2");
		if(fsrv == FSReturnVals.DirNotEmpty){
			System.out.println("Good!  Detected /" + dir2 + "/1/2 exists.");
		} else {
			System.out.println("Unit test 2 result: fail! - 4");
    		return;
		}
		
		
		
		for(int i = 1; i < N; i++){
			fsrv = cfs.RenameDir("/" + dir1 + "/" + i, "/" + dir1 + "/" + i + "i");
			System.out.println("UT2 RenameResult: "+fsrv);
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
		if(arr == null || arr.length == 0){
		    return false;
		}
		
		for (int i=0; i < arr.length; i++)
			if (arr[i].equals(token)) return true;
		return false;
	}

}
