//buffer.java 

import java.util.HashMap;

class Buffer{
	
	public int numberOf_row; 
	public HashMap<Integer, String[]> content;
	public String[] name_of_the_column; 
	public int[] format_required; 
	
	public Buffer(){
		numberOf_row = 0;
		content = new HashMap<Integer, String[]>();
	}

	public void add_vals(int rowid, String[] val){
		content.put(rowid, val);
		numberOf_row = numberOf_row + 1;
	}

	public String fix(int len, String s) {
		return String.format_required("%-"+(len+3)+"s", s);
	}


	public void display(String[] col){
		
		if(numberOf_row == 0){
			System.out.println("Empty set.");
		}
		else{
			for(int i = 0; i < format_required.length; i++)
				format_required[i] = name_of_the_column[i].length();
			for(String[] i : content.values())
				for(int j = 0; j < i.length; j++)
					if(format_required[j] < i[j].length())
						format_required[j] = i[j].length();
			
			if(col[0].equals("*")){
				
				for(int l: format_required)
					System.out.print(DavisBase.line("-", l+3));
				
				System.out.println();
				
				for(int i = 0; i< name_of_the_column.length; i++)
					System.out.print(fix(format_required[i], name_of_the_column[i])+"|");
				
				System.out.println();
				
				for(int l: format_required)
					System.out.print(DavisBase.line("-", l+3));
				
				System.out.println();

				for(String[] i : content.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format_required[j], i[j])+"|");
					System.out.println();
				}
			
			}
			else{
				int[] controlling = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < name_of_the_column.length; i++)
						if(col[j].equals(name_of_the_column[i]))
							controlling[j] = i;

				for(int j = 0; j < controlling.length; j++)
					System.out.print(DavisBase.line("-", format_required[controlling[j]]+3));
				
				System.out.println();
				
				for(int j = 0; j < controlling.length; j++)
					System.out.print(fix(format_required[controlling[j]], name_of_the_column[controlling[j]])+"|");
				
				System.out.println();
				
				for(int j = 0; j < controlling.length; j++)
					System.out.print(DavisBase.line("-", format_required[controlling[j]]+3));
				
				System.out.println();
				
				for(String[] i : content.values()){
					for(int j = 0; j < controlling.length; j++)
						System.out.print(fix(format_required[controlling[j]], i[controlling[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}