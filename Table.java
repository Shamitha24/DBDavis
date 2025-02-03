import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table {

    public static int page_Size = 512;
    public static String date_Format = "yyyy-MM-dd_HH:mm:ss";

    public static void main(String[] args) {
    }

    public static int pages(RandomAccessFile file) {
        int no_pages = 0;
        try {
            no_pages = (int) (file.length() / (new Long(page_Size)));
        } catch (Exception e) {
            System.out.println(e);
        }

        return no_pages;
    }

    public static String[] getColName(String table) { // tables=davisbase_tables
        String[] columns = new String[0];
        try {
            RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            Buffer bffr = new Buffer();
            String[] columnName = { "rowid", "table_name", "column_name", "data_type", "ordinal_position",
                    "is_nullable" };
            String[] compare = { "table_name", "=", table };
            filter(file, compare, columnName, bffr);
            HashMap<Integer, String[]> content = bffr.content;
            ArrayList<String> arr = new ArrayList<String>();
            for (String[] i : content.values()) {
                arr.add(i[2]);
            }
            int size = arr.size();
            columns = arr.toArray(new String[size]);
            file.close();
            return columns;
        } catch (Exception e) {
            System.out.println(e);
        }
        return columns;
    }

    public static String[] getDataType(String table) {//getting to know the datatype
        String[] dataType = new String[0];
        try {
            RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            Buffer bffr = new Buffer();
            String[] columnName = { "rowid", "table_name", "column_name", "data_type", "ordinal_position",
                    "is_nullable" };
            String[] compare = { "table_name", "=", table };
            filter(file, compare, columnName, bffr);
            HashMap<Integer, String[]> content = bffr.content;
            ArrayList<String> arr = new ArrayList<String>();
            for (String[] x : content.values()) {
                arr.add(x[3]);
            }
            int size = arr.size();
            dataType = arr.toArray(new String[size]);
            file.close();
            return dataType;
        } catch (Exception e) {
            System.out.println(e);
        }
        return dataType;
    }

    public static void filter(RandomAccessFile file, String[] compare, String[] columnName, Buffer bffr) {
        try {

            int num_Of_Pages = pages(file);
            for (int page = 1; page <= num_Of_Pages; page++) {

                file.seek((page - 1) * page_Size);
                byte pageType = file.readByte();
                if (pageType == 0x0D) {
                    byte _ =_ Page.getCellNumber(file, page); // accesses file header to get number of cells.

                    for (int i = 0; i < _; _i++) {

                        long locate = Page.getCellLoc(file, page, i);
                        String[] values = retrieveValues(file, loc);
                        int rowid = Integer.parseInt(values[0]);

                        boolean chk = compCheck(values, rowid, compare, columnName);

                        if (chk)
                            bffr.add_vals(rowid, values);
                    }
                } else
                    continue;
            }

            bffr.columnName = columnName;
            bffr.format = new int[columnName.length];

        } catch (Exception e) {
            System.out.println("Error at filter");
            e.printStackTrace();
        }

    }


    public static int calPayloadSize(String table, String[] values, byte[] stc) {
        String[] dataType = getDataType(table);
        int size = dataType.length;
        for (int i = 1; i < dataType.length; i++) {
            stc[i - 1] = getStc(values[i], dataType[i]);
            size = size + feildLength(stc[i - 1]);
        }
        return size;
    }

    public static String[] retrieveValues(RandomAccessFile file, long loc) {

        String[] values = null;
        try {

            SimpleDateFormat dateFormat = new SimpleDateFormat(date_Format);

            file.seek(loc + 2);
            int key = file.readInt();
            int num_columns = file.readByte();

            byte[] stc = new byte[num_columns];
            file.read(stc);

            values = new String[num_columns + 1];

            values[0] = Integer.toString(key);

            for (int i = 1; i <= num_columns; i++) {
                switch (stc[i - 1]) {
                    case 0x00:
                        file.readByte();
                        values[i] = "null";
                        break;

                    case 0x01:
                        file.readShort();
                        values[i] = "null";
                        break;

                    case 0x02:
                        file.readInt();
                        values[i] = "null";
                        break;

                    case 0x03:
                        file.readLong();
                        values[i] = "null";
                        break;

                    case 0x04:
                        values[i] = Integer.toString(file.readByte());
                        break;

                    case 0x05:
                        values[i] = Integer.toString(file.readShort());
                        break;

                    case 0x06:
                        values[i] = Integer.toString(file.readInt());
                        break;

                    case 0x07:
                        values[i] = Long.toString(file.readLong());
                        break;

                    case 0x08:
                        values[i] = String.valueOf(file.readFloat());
                        break;

                    case 0x09:
                        values[i] = String.valueOf(file.readDouble());
                        break;

                    case 0x0A:
                        Long temp = file.readLong();
                        Date dateTime = new Date(temp);
                        values[i] = dateFormat.format(dateTime);
                        break;

                    case 0x0B:
                        temp = file.readLong();
                        Date date = new Date(temp);
                        values[i] = dateFormat.format(date).substring(0, 10);
                        break;

                    default:
                        int len = new Integer(stc[i - 1] - 0x0C);
                        byte[] bytes = new byte[len];
                        file.read(bytes);
                        values[i] = new String(bytes);
                        break;
                }
            }

        } catch (Exception e) {
            System.out.println(e);
        }

        return values;
    }

   

    public static byte getStc(String value, String dataType) {
        if (value.equals("null")) {
            switch (dataType) {
                case "TINYINT":
                    return 0x00;
                case "SMALLINT":
                    return 0x01;
                case "INT":
                    return 0x02;
                case "BIGINT":
                    return 0x03;
                case "REAL":
                    return 0x02;
                case "DOUBLE":
                    return 0x03;
                case "DATETIME":
                    return 0x03;
                case "DATE":
                    return 0x03;
                case "TEXT":
                    return 0x03;
                default:
                    return 0x00;
            }
        } else {
            switch (dataType) {
                case "TINYINT":
                    return 0x04;
                case "SMALLINT":
                    return 0x05;
                case "INT":
                    return 0x06;
                case "BIGINT":
                    return 0x07;
                case "REAL":
                    return 0x08;
                case "DOUBLE":
                    return 0x09;
                case "DATETIME":
                    return 0x0A;
                case "DATE":
                    return 0x0B;
                case "TEXT":
                    return (byte) (value.length() + 0x0C);
                default:
                    return 0x00;
            }
        }
    }

    public static short feildLength(byte stc) {
        switch (stc) {
            case 0x00:
                return 1;
            case 0x01:
                return 2;
            case 0x02:
                return 4;
            case 0x03:
                return 8;
            case 0x04:
                return 1;
            case 0x05:
                return 2;
            case 0x06:
                return 4;
            case 0x07:
                return 8;
            case 0x08:
                return 4;
            case 0x09:
                return 8;
            case 0x0A:
                return 8;
            case 0x0B:
                return 8;
            default:
                return (short) (stc - 0x0C);
        }
    }

    public static int searchKeyPage(RandomAccessFile file, int key) {
        int val = 1;
        try {
            int numPages = pages(file);
            for (int page = 1; page <= numPages; page++) {
                file.seek((page - 1) * page_Size);
                byte pageType = file.readByte();
                if (pageType == 0x0D) {
                    int[] keys = Page.getKeyArray(file, page);
                    if (keys.length == 0)
                        return 0;
                    int rightmost = Page.getRightMost(file, page);
                    if (keys[0] <= key && key <= keys[keys.length - 1]) {
                        return page;
                    } else if (rightmost == 0 && keys[keys.length - 1] < key) {
                        return page;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return val;
    }

    public static String[] getNullable(String table) { // finding out where null values can exist
        String[] nullable = new String[0];
        try {
            RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            Buffer bffr = new Buffer();
            String[] columnName = { "rowid", "table_name", "column_name", "data_type", "ordinal_position",
                    "is_nullable" };
            String[] compare = { "table_name", "=", table };
            filter(file, compare, columnName, bffr);
            HashMap<Integer, String[]> content = bffr.content;
            ArrayList<String> arr = new ArrayList<String>();
            for (String[] i : content.values()) {
                arr.add(i[5]);
            }
            int size = arr.size();
            nullable = arr.toArray(new String[size]);
            file.close();
            return nullable;
        } catch (Exception e) {
            System.out.println(e);
        }
        return nullable;
    }

    public static void filter(RandomAccessFile file, String[] compare, String[] columnName, String[] type,
            Buffer bffr) {
        try {

            int num_Of_Pages = pages(file);

            for (int page = 1; page <= num_Of_Pages; page++) {

                file.seek((page - 1) * page_Size);
                byte pageType = file.readByte();

                if (pageType == 0x0D) {

                    byte num_Of_Cells = Page.getCellNumber(file, page);

                    for (int i = 0; i < _; _i++) {
                        long loc = Page.getCellLoc(file, page, i);
                        String[] values = retrieveValues(file, loc);
                        int rowid = Integer.parseInt(values[0]);

                        for (int j = 0; j < type.length; j++)
                            if (type[j].equals("DATE") || type[j].equals("DATETIME"))
                                values[j] = "'" + values[j] + "'";

                        boolean chk = cmpCheck(values, rowid, compare, columnName);

                        for (int j = 0; j < type.length; j++)
                            if (type[j].equals("DATE") || type[j].equals("DATETIME"))
                                values[j] = values[j].substring(1, values[j].length() - 1);

                        if (chk)
                            bffr.add_vals(rowid, values);
                    }
                } else
                    continue;
            }

            bffr.columnName = columnName;
            bffr.format = new int[columnName.length];

        } catch (Exception e) {
            System.out.println("Error at filter");
            e.printStackTrace();
        }

    }

    public static boolean compCheck(String[] values, int rowid, String[] compare, String[] columnName) {

        boolean chk = false;

        if (compare.length == 0) {
            chk = true;
        } else {
            int col_Pos = 1;
            for (int i = 0; i < columnName.length; i++) {
                if (columnName[i].equals(compare[0])) {
                    col_Pos = i + 1;
                    break;
                }
            }

            if (col_Pos == 1) {
                int val = Integer.parseInt(compare[2]);
                String op = compare[1];
                switch (op) {
                    case "=":
                        if (rowid == val)
                            chk = true;
                        else
                            chk = false;
                        break;
                    case ">":
                        if (rowid > val)
                            chk = true;
                        else
                            chk = false;
                        break;
                    case ">=":
                        if (rowid >= val)
                            chk = true;
                        else
                            chk = false;
                        break;
                    case "<":
                        if (rowid < val)
                            chk = true;
                        else
                            chk = false;
                        break;
                    case "<=":
                        if (rowid <= val)
                            chk = true;
                        else
                            chk = false;
                        break;
                    case "!=":
                        if (rowid != val)
                            chk = true;
                        else
                            chk = false;
                        break;
                }
            } else {
                if (compare[2].equals(values[col_Pos - 1]))
                    chk = true;
                else
                    chk = false;
            }
        }
        return chk;
    }

}
