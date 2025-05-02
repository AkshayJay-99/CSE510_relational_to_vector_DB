import diskmgr.DB;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import lshfindex.*;
import java.util.Scanner;

public class Interface {

    // Global variable to store the currently open database name
    private static String currentDatabaseName = null;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("MiniBase VectorDB Interface - Type 'exit' to quit.");

        while (true) {
            System.out.print(">> ");
            String input = scanner.nextLine().trim();
            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Exiting...");
                break;
            }

            String[] tokens = input.split("\\s+");
            if (tokens.length == 0) continue;

            String command = tokens[0].toLowerCase();

            try {
                switch (command) {
                    case "open":
                        if (tokens.length >= 2 && tokens[1].equalsIgnoreCase("database")) {
                            openDatabase(tokens[2]);
                        } else {
                            System.out.println("Usage: open database DBNAME");
                        }
                        break;

                    case "close":
                        if (tokens.length == 2 && tokens[1].equalsIgnoreCase("database")) {
                            closeDatabase();
                        } else {
                            System.out.println("Usage: close database");
                        }
                        break;

                    case "batchcreate":
                        if (requireDatabaseContext() && tokens.length == 3) {
                            batchCreate(tokens[1], tokens[2]);
                        } else {
                            System.out.println("Usage: batchcreate DATAFILENAME RELNAME");
                        }
                        break;

                    case "createindex":
                        if (requireDatabaseContext() && tokens.length == 5) {
                            createIndex(tokens[1], Integer.parseInt(tokens[2]),
                                    Integer.parseInt(tokens[3]), Integer.parseInt(tokens[4]));
                        } else {
                            System.out.println("Usage: createindex RELNAME COLUMNID L h");
                        }
                        break;

                    case "batchinsert":
                        if (requireDatabaseContext() && tokens.length == 3) {
                            batchInsert(tokens[1], tokens[2]);
                        } else {
                            System.out.println("Usage: batchinsert UPDATEFILENAME RELNAME");
                        }
                        break;

                    case "batchdelete":
                        if (requireDatabaseContext() && tokens.length == 3) {
                            batchDelete(tokens[1], tokens[2]);
                        } else {
                            System.out.println("Usage: batchdelete UPDATEFILENAME RELNAME");
                        }
                        break;

                    case "query":
                        if (requireDatabaseContext() && tokens.length == 5) {
                            query(tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4]));
                        } else {
                            System.out.println("Usage: query RELNAME1 RELNAME2 QSNAME NUMBUF");
                        }
                        break;

                    default:
                        System.out.println("Unknown command.");
                        break;
                }
            } catch (Exception e) {
                System.out.println("Error while executing command: " + e.getMessage());
                e.printStackTrace();
            }
        }

        scanner.close();
    }
    public static void flushPages() {
        try {
            SystemDefs.JavabaseBM.flushAllPages(); // Assuming there's a method to flush all pages
            System.out.println("All pages flushed to disk.");
        } catch (Exception e) {
            System.err.println("Error flushing pages: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean requireDatabaseContext() {
        if (currentDatabaseName == null) {
            System.out.println("No database is currently open. Use 'open database DBNAME' first.");
            return false;
        }
        return true;
    }

    // --- Command handler stubs ---

    public static void openDatabase(String dbName) {
        currentDatabaseName = dbName;
        String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName; 
        int numPages = 12000; // Disk pages allocated
        int bufferSize = 16000; // Buffer pool size
        new SystemDefs(dbpath, 0, bufferSize, "Clock");
        if (SystemDefs.JavabaseDB.db_num_pages() == 0) {
            System.out.println("Database is empty. Creating new database.");
            new SystemDefs(dbpath, numPages, bufferSize, "Clock");
        } else {
            System.out.println("Database already exists. Opened successfully.");
        }
        System.out.println("Number of pages in the database: " + SystemDefs.JavabaseDB.db_num_pages());
    }

    public static void closeDatabase() {
        try{
        if (currentDatabaseName != null) {
            System.out.println("Closing database: " + currentDatabaseName);
            flushPages();
            SystemDefs.JavabaseDB.closeDB();  // Close the database
            currentDatabaseName = null;
        } else {
            System.out.println("No open database to close.");
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void batchCreate(String dataFile, String relName) {
        System.out.println("Creating table '" + relName + "' from file: " + dataFile + " in database: " + currentDatabaseName);

        int h = 0; // Number of hash functions per layer -- initialized to 0
        int L = 0; // Number of layers
        try {

            BufferedReader reader = new BufferedReader(new FileReader(dataFile));
            int numAttributes = Integer.parseInt(reader.readLine().trim()); // Read first line (number of attributes)
            String[] attrTypes = reader.readLine().trim().split("\\s+"); // Read second line (attribute types)
            // 🔹 Step 3: Define Schema
            String attr_char = "";
            AttrType[] schema = new AttrType[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                int typeCode = Integer.parseInt(attrTypes[i]);
                attr_char += (char) (typeCode+ '0');
                switch (typeCode) {
                    case 1: schema[i] = new AttrType(AttrType.attrInteger); break;
                    case 2: schema[i] = new AttrType(AttrType.attrReal); break;
                    case 3: schema[i] = new AttrType(AttrType.attrString); break;
                    case 4: schema[i] = new AttrType(AttrType.attrVector100D); break;
                    default: throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
                }
            }
            attr_char +=(char) (h+ '0');
            attr_char +=(char) (L+ '0');
            System.out.println(attr_char);
            Heapfile heapfile_sc = new Heapfile("sc_"+relName+".in");
            Tuple sc_tuple = new Tuple();
            sc_tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
            sc_tuple.setStrFld(1, attr_char);  
            RID sc_rid = heapfile_sc.insertRecord(sc_tuple.getTupleByteArray());
            System.out.println("Schema stored with RID: Page " + sc_rid.pageNo.pid + ", Slot " + sc_rid.slotNo);

            // 🔹 Step 4: Create Heapfile
            Heapfile heapfile = new Heapfile(relName+".in");
            heapfile.deleteFile();
            heapfile = new Heapfile(relName+".in");
            // 🔹 Step 5b: Read and insert tuples
            String line;
            while ((line = reader.readLine()) != null) {
                Tuple tuple = new Tuple();
                tuple.setHdr((short) numAttributes, schema, getStringSizes(schema));
                int fieldNum = 1;
                for (int i = 0; i < numAttributes; i++) {
                    switch (schema[i].attrType) {
                        case AttrType.attrInteger:
                            //System.out.println("Attr Integer: "+line.trim());
                            tuple.setIntFld(fieldNum, Integer.parseInt(line.trim()));

                            break;

                        case AttrType.attrReal:
                            //System.out.println("Attr Real: "+line.trim());
                            tuple.setFloFld(fieldNum, Float.parseFloat(line.trim()));

                            break;

                        case AttrType.attrString:
                            //System.out.println("Attr String: "+line.trim());
                            tuple.setStrFld(fieldNum, line.trim()); // Handles single or double-quoted names

                            break;

                        case AttrType.attrVector100D:
                            short[] vector = new short[100];
                            String[] vectorValues = line.trim().split("\\s+"); // Read 100 value
                             for (int j = 0; j < 100; j++) {
                                vector[j] =(short) Short.parseShort(vectorValues[j]); //Change the value of short to int
                           }
                            tuple.set100DVectorFld(fieldNum, new Vector100Dtype(vector));
                            break;
                    }
                    fieldNum++;
                    if (i < numAttributes - 1) line = reader.readLine(); // Read next attribute
                }
            
                // Insert tuple into heap file
                RID recordID =  heapfile.insertRecord(tuple.getTupleByteArray());
            }

            reader.close();
            //System.out.println("Batch Insertion Complete!");
            flushPages();
            
            // Optionally, here's how you could shut down the system entirely, which also ensures data is saved
            // 🔹 Step 6: Output disk usage stats (added here!)
            System.out.println("Disk pages read: " + PCounter.rcounter);
            System.out.println("Disk pages written: " + PCounter.wcounter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void createIndex(String relName, int columnId, int L, int h) {
        System.out.println("[Creating index on " + relName + ", column " + columnId +
                ", L=" + L + ", h=" + h + " in database: " + currentDatabaseName);
        // TODO: Implement index creation logic
    }

    public static void batchInsert(String updateFile, String relName) {
        System.out.println("Inserting data into " + relName + " from file: " + updateFile +
                " in database: " + currentDatabaseName);
        // TODO: Implement batch insert logic
    }

    public static void batchDelete(String updateFile, String relName) {
        System.out.println("Deleting from " + relName + " using: " + updateFile +
                " in database: " + currentDatabaseName);
        // TODO: Implement batch delete logic
    }

    public static void query(String rel1, String rel2, String qsName, int numBuf) {
        System.out.println("Running external Query.java program...");

        try {
            Query.main(rel1, rel2, qsName, numBuf);

            System.out.println("Query completed.");
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static short[] getStringSizes(AttrType[] schema) {
        int count = 0;
        for (AttrType attr : schema) {
            if (attr.attrType == AttrType.attrString) count++;
        }
        short[] strSizes = new short[count];
        Arrays.fill(strSizes, (short) 30); // Default string length
        return strSizes;
    }


}
