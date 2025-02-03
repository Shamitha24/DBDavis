import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.SortedMap;
public class DeleteTable {
	public static void parseDeleteString(String deleteString) {
		System.out.println("DELETE METHOD");
		System.out.println("Parsing the string:\"" + deleteString + "\"");
		
		String[] tokens=deleteString.split(" ");
		String table = tokens[3];
		String[] tmp = deleteString.split("where");  // condition checking
		String compare_tmp = tmp[1];
		String[] compare = DavisBase.parserEquation(compare_tmp);
		if(!DavisBase.tableExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			delete(table, compare);
		}
	}
	public static void delete(String table, String[] compare){
		try{
		int key = new Integer(compare[2]);

		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		int no_Pages = Table.pages(file);
		int page = 0;
		for(int p = 1; p <= no_Pages; p++)
			if(Page.hasKey(file, p, key)&Page.getPageType(file, p)==0x0D){
				page = p;
				break;
			}
		// for given query if page does not exist 
		if(page==0)
		{
			System.out.println("The given key value does not exist");
			return;
		}
		
		short[] cell_Addy = Page.getCellArray(file, page);
		int k = 0;
		for(int i = 0; i < cell_Addy.length; i++)
		{
			long locate = Page.getCellLoc(file, page, i);
			String[] ans = Table.retrieveValues(file, locate);
			int x = new Integer(ans[0]);
			if(x!=key)
			{
				Page.setCellOffset(file, page, k, cell_Addy[i]);
				k++;
			}
		}
		Page.setCellNumber(file, page, (byte)k);
		
		}catch(Exception e)
		{
			System.out.println(e);
		}
		
	}

}
