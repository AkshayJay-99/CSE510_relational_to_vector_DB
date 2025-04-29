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
        // TODO: Implement table creation logic
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
            ProcessBuilder pb = new ProcessBuilder(
                    "java", "Query", rel1, rel2, qsName, String.valueOf(numBuf), currentDatabaseName
            );

            pb.inheritIO(); // Stream output of Query.java directly to console
            Process process = pb.start();
            process.waitFor(); // Block until Query.java completes

            System.out.println("Query completed.");
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
