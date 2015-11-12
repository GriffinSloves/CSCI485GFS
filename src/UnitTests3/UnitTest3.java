package UnitTests3;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;

/**
 * UnitTest3 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest3 {
	
	public static int N = 755;

	public static void main(String[] args) {
		ClientFS cfs = new ClientFS();
		UnitTest2 ut2 = new UnitTest2();
		ut2.test2(cfs);

		String dir1 = "ShahramGhandeharizadeh";
		FSReturnVals fsrv = cfs.CreateDir("/" + dir1 + "/", "CSCI485");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 3 result: fail!");
    		return;
		}
		String[] gen1 = new String[N];
		for(int i = 1; i <= N; i++){
			fsrv = cfs.CreateFile("/" + dir1 + "/CSCI485/", "Lecture" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!");
	    		return;
			}
		}
		for(int i = 1; i <= N; i++){
			fsrv = cfs.DeleteFile("/" + dir1 + "/CSCI485/", "Lecture" + i);
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!");
	    		return;
			}
		}
		
		String dir2 = "Shahram";
		for(int i = 1; i <= N; i++){
			fsrv = cfs.CreateFile("/" + dir2 + "/2" + i + "/", "Lecture" + i);
			//fsrv = cfs.CreateFile("/" + dir2 + "/2/", "Lecture" + i); --Correction From TA
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!");
	    		return;
			}
		}
		
		for(int i = 1; i <= N; i++){
			fsrv = cfs.CreateFile("/" + dir2 + "/2" + i + "/", "Lecture" + i);
			//fsrv = cfs.DeleteFile("/" + dir2 + "/2/", "Lecture" + i); --Correction from TA
			if( fsrv != FSReturnVals.Success ){
				System.out.println("Unit test 3 result: fail!");
	    		return;
			}
		}
		
		System.out.println("Unit test 2 result: success!");
	}
}
