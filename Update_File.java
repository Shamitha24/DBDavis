//UpdateTable.java

import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class UpdateTable {
	public static void parseUpdateString(String updateString) {
		System.out.println("UPDATE METHOD");
		System.out.println("Parsing the string:\"" + updateString + "\"");
		
		String[] token_taken=updateString.split(" ");
		String table_for_the_file = token_taken[1];
		String[] temporary1 = updateString.split("set");
		String[] temporary2 = temporary1[1].split("where");
		String cmpTemp = temporary2[1];
		String settingTemporary = temporary2[0];
		String[] cmp = DavisBase.parserEquation(cmpTemp);
		String[] set = DavisBase.parserEquation(settingTemporary);
		if(!DavisBase.tableExists(table_for_the_file)){
			System.out.println("Table "+table_for_the_file+" does not exist.");
		}
		else
		{
			update(table_for_the_file, cmp, set);
		}
		
	}
	public static void update(String table_for_the_file, String[] cmp, String[] set){
		try{
			
			int key_names = new Integer(cmp[2]);
			
			RandomAccessFile file = new RandomAccessFile("data/"+table_for_the_file+".tbl", "rw");
			int number_of_page = Table.pages(file);
			int page = 0;
			for(int p = 1; p <= number_of_page; p++)
				if(Page.haskey_names(file, p, key_names)&Page.getPageType(file, p)==0x0D){
					page = p;
				}
			
			if(page==0)
			{
				System.out.println("The given key_names value does not exist");
				return;
			}
			
			int[] key_namess = Page.getkey_namesArray(file, page);
			int x = 0;
			for(int i = 0; i < key_namess.length; i++)
				if(key_namess[i] == key_names)
					x = i;
			int offset = Page.getCellOffset(file, page, x);
			long location = Page.getCelllocation(file, page, x);
			
			String[] column = Table.getColName(table_for_the_file);
			String[] values = Table.retrieveValues(file, location);

			String[] type = Table.getDataType(table_for_the_file);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";

			for(int i = 0; i < column.length; i++)
				if(column[i].equals(set[0]))
					x = i;
			values[x] = set[2];

			String[] nullable = Table.getNullable(table_for_the_file);
			for(int i = 0; i < nullable.length; i++){
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL-value constraint violation");
					return;
				}
			}

			byte[] stc = new byte[column.length-1];
			int sizeofthe_pl = Table.calPayloadSize(table_for_the_file, values, stc);
			Page.updateLeafCell(file, page, offset, sizeofthe_pl, key_names, stc, values);

			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}

}