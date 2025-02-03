import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.SortedMap;
public class DropTable {
	public static void dropTable(String dropTableString) {
		System.out.println("DROP METHOD");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		
		String[] vector=dropTableString.split(" ");
		String tableName = vector[2];
		if(!DavisBase.tableExists(tableName)){//checking if table does not exist
			System.out.println("Table "+tableName+" does not exist.");
		}
		else
		{
			drop(tableName);
		}		

	}
	public static void drop(String table){
		//code snippet showing process of deleting
        try{
			
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int no_Pages = Table.pages(file);
			for(int page = 1; page <= no_Pages; page ++){
				file.seek((page-1)*Table.pageSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] cellAddy = Page.getCellArray(file, page);
					int k = 0;
					for(int i = 0; i < cellAddy.length; i++)
					{
						long locate = Page.getCellLoc(file, page, i);
						String[] values = Table.retrieveValues(file, locate);
						String tbl = values[1];
						if(!tbl.equals(table))
						{
							Page.setCellOffset(file, page, k, cellAddy[i]);
							k++;
						}
					}
					Page.setCellNumber(file, page, (byte)k);
				}
				else
					continue;
			}

			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			no_Pages = Table.pages(file);
			for(int page = 1; page <= no_Pages; page ++){
				file.seek((page-1)*Table.pageSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] cellAddy = Page.getCellArray(file, page);
					int k = 0;
					for(int i = 0; i < cellAddy.length; i++)
					{
						long locate = Page.getCellLoc(file, page, i);
						String[] values = Table.retrieveValues(file, locate);
						String tbl = values[1];
						if(!tbl.equals(table))
						{
							Page.setCellOffset(file, page, k, cellAddy[i]);
							k++;
						}
					}
					Page.setCellNumber(file, page, (byte)k);
				}
				else
					continue;
			}

			File existingFile = new File("data", table+".tbl"); 
			existingFile.delete();
		}catch(Exception e){
			System.out.println(e);
		}

	}

}