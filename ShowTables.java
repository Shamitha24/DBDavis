import java.io.RandomAccessFile;
public class ShowTables{

public static void showTables() {
		System.out.println("SHOW METHOD");
		System.out.println("Parsing the string:\"show tables\"");
		
		String table = "davisbase_tables";
		String[] cols = {"table_name"};
		String[] cmptr = new String[0];
		select(table, cols, cmptr); 
	}

public static void select(String table, String[] cols, String[] cmp){     //method for select command
	try{
		
		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		String[] columnName = Table.getColName(table);
		String[] type = Table.getDataType(table);
		
		Buffer buffer = new Buffer();
		
		Table.filter(file, cmp, columnName, type, buffer);
		buffer.display(cols);
		file.close();
	}catch(Exception e){
		System.out.println(e);
	}
}

}
