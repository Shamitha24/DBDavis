import java.io.RandomAccessFile;
import java.util.Scanner;
import java.util.Date;
import java.text.SimpleDateFormat;

public class CreateTable {

    public static void parseCreateString(String createString) {
        System.out.println("CREATE METHOD");
        System.out.println("Parsing the string:\"" + createString + "\"");
        String[] token_name = createString.split(" ");

        //checking if the index creation command exists

        if  token_name[1].compareTo(("index") == 0) {
            String name_of_the_index = token_name[2]; //index name is the second token in here
            String table_name = token_name[3]; // here the table anme is the fourth token
            String column_taken = token_name[4];       // the column name is the fifth token
            String colName = column_taken.substring(1, column_taken.length() - 1); //removing quotes from column name

            //creation of the index
            Index.createIndex(table_name, name_of_the_index, colName, "String");
        } else {
            //check if it's a table creation command
            if  token_name[1].compareTo(("table") > 0) {
                System.out.println("Wrong syntax");
            } else {
                String table_name = token_name[2]; //table name is the second token
                String[] temporary = createString.split(table_name);
                String cols = temporary[1].trim();
                String[] creating_columns = cols.substring(1, cols.length() - 1).split(",");

                for (int i = 0; i < creating_columns.length; i++)
                    creating_columns[i] = creating_columns[i].trim();

                //we need to check if the table exists already
                if (DavisBase.tableExists(table_name)) {
                    System.out.println("Table " + table_name + " already exists.");
                } else {
                    createTable(table_name, creating_columns);
                }
            }
        }
    }

    public static void createTable(String table, String[] column_taken) {
        try {
            // creating table file (.tbl)
            RandomAccessFile file = new RandomAccessFile("data/" + table + ".tbl", "rw");
            file.setLength(Table.pageSize);
            file.seek(0);
            file.writeByte(0x0D); // Write
            file.close();

            //insertion into the davisbase_tables
            file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
            int number_of_pages = Table.pages(file);
            int page = 1;
            for (int p = 1; p <= number_of_pages; p++) {
                int rm = Page.getRightMost(file, p);
                if (rm == 0)
                    page = p;
            }

            int[] key_name = Page.getKeyArray(file, page);
            int l = key_name[0];
            for (int i = 0; i < key_name.length; i++)
                if (key_name[i] > l)
                    l = key_name[i];
            file.close();

            String[] value_requireds = {Integer.toString(l + 1), table};
            insertInto("davisbase_tables", value_requireds);

            //insert into davisbase_columns with rowid
            file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            number_of_pages = Table.pages(file);
            page = 1;
            for (int p = 1; p <= number_of_pages; p++) {
                int rm = Page.getRightMost(file, p);
                if (rm == 0)
                    page = p;
            }

            key_name = Page.getKeyArray(file, page);
            l = key_name[0];
            for (int i = 0; i < key_name.length; i++)
                if (key_name[i] > l)
                    l = key_name[i];
            file.close();

            //hidden rowid as the first column
            String[] rowIDColumn = {"rowid INTEGER NOT NULL"};

            //combing the rowid column column taken 
            String[] creating_columns_with_rowid = concatenate(rowIDColumn, column_taken);

            for (int i = 0; i < creating_columns_with_rowid.length; i++) {
                l = l + 1;
                String[] token = creating_columns_with_rowid[i].split(" ");
                String name_of_col = token[0];
                String dt = token[1].toUpperCase();
                String pos = Integer.toString(i + 1);
                String isitNull = "YES";
                if (token.length > 2) {
                    isitNull = "NO";
                }

                //checking if the PRIMARY KEY constraint and uniqueness exists
                if (name_of_col.equals("rowid")) {
                    continue; //we must skip the rowid from primary key check
                }
                if (token.length == 3 && token[2].equals("PRIMARY") && i == 0) {
                    System.out.println("PRIMARY KEY constraint violation: only one column can be PRIMARY KEY.");
                    return;
                }

                String[] value_required = {Integer.toString(l), table, name_of_col, dt, pos, isitNull};
                insertInto("davisbase_columns", value_required);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static String[] concatenate(String[] a, String[] b) {
        String[] results = new String[a.length + b.length];
        System.arraycopy(a, 0, results, 0, a.length);
        System.arraycopy(b, 0, results, a.length, b.length);
        return results;
    }

    public static void insertInto(String table, String[] value_requireds) {
        try {
            RandomAccessFile file = new RandomAccessFile("data/" + table + ".tbl", "rw");
            insertInto(file, table, value_requireds);
            file.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void insertInto(RandomAccessFile file, String table, String[] value_requireds) {
        String[] dtype = Table.getDataType(table);
        String[] isitNull = Table.getisitNull(table);

        for (int i = 0; i < isitNull.length; i++)
            if (value_requireds[i].equals("null") && isitNull[i].equals("NO")) {
                System.out.println("NULL-value_required constraint violation");
                System.out.println();
                return;
            }

        int key = new Integer(value_requireds[0]);
        int page = Table.searchKeyPage(file, key);
        if (page != 0)
            if (Page.hasKey(file, page, key)) {
                System.out.println("Uniqueness constraint violation");
                return;
            }
        if (page == 0)
            page = 1;

        byte[] stc = new byte[dtype.length - 1];
        short plSize = (short) Table.calPayloadSize(table, value_requireds, stc);
        int size_of_the_cell = plSize + 6;
        int offset = Page.checkLeafSpace(file, page, size_of_the_cell);

        if (offset != -1) {
            Page.insertLeafCell(file, page, offset, plSize, key, stc, value_requireds);
        } else {
            Page.splitLeaf(file, page);
            insertInto(file, table, value_requireds);
        }
    }
}