package lshfindex;
import java.util.Random;
import global.*;
import java.util.*;
import java.util.Arrays;

public class LSHFTest {
    
    public static void main(String[] args) {
        try {
            // You can run this test file using these commands
            //javac -cp . LSHFIndex/*.java global/*.java heap/*.java diskmgr/*.java index/*.java btree/*.java bufmgr/*.java iterator/*.java
            //java -cp . lshfindex.LSHFTest

            // // Step 1: Initialize MiniBase & Create LSH Index
            // SystemDefs sysdef = new SystemDefs("minibase.lshftest", 5000, 2000, "Clock");
            // LSHFFile lshIndex = new LSHFFile("lsh_test_index", 5, 3); // h=5, L=3
            // Random random = new Random(42);
            // short[] queryVec = new short[100];
            // // Step 2: Insert Sample Vectors
            // //System.out.println("Inserting Vectors");
            // for (int i = 0; i < 50; i++) {
            //     short[] vec = new short[100];
            //     //Arrays.fill(vec, (short) (i * 10)); // Simple increasing pattern
            //     for(int j = 0; j < 100; j++)
            //     {
            //         vec[j] = (short) random.nextInt(10000);
            //     }
            //     Vector100Dtype key = new Vector100Dtype(vec);
            //     RID rid = new RID(); // Fake RID for testing
            //     //System.out.println("Creating fresh RID: Page " + rid.pageNo.pid + ", Slot " + rid.slotNo);
            //     queryVec = vec;
            //     lshIndex.insert(key, rid);
            //     //System.out.println("                                                Inserted vector " + (i));
            // }

            // // Step 3: Run Nearest Neighbor Search
            // //short[] queryVec = new short[100];
            // Vector100Dtype queryKey = new Vector100Dtype(queryVec);

            // List<RID> nearest = lshIndex.NN_Search(queryKey, 16);
            // System.out.println("\nTop-5 Nearest Neighbors:");
            // for (RID rid : nearest) {
            //     System.out.println("RID: Page " + rid.pageNo.pid + ", Slot " + rid.slotNo);
            // }


            //  //System.out.println("Printing query array: " + Arrays.toString(queryKey.getValues()));
            // // Step 4: Run Range Search
            // List<RID> rangeResults = lshIndex.rangeSearch(queryKey, 1000);
            // System.out.println("\nRange Search Results (Distance <= 50):");
            // for (RID rid : rangeResults) {
            //     System.out.println("RID: Page " + rid.pageNo.pid + ", Slot " + rid.slotNo);
            // }

                //         // Step 1: Initialize MiniBase & Create LSH Index
                // SystemDefs sysdef = new SystemDefs("minibase.lshftest", 5000, 2000, "Clock");
                // LSHFFile lshIndex = new LSHFFile("lsh_test_index", 5, 3); // h=5, L=3
                // Random random = new Random(42);

                // // Step 2: Insert Sample Vectors
                // for (int i = 0; i < 10; i++) { // Inserting fewer records for easier debugging
                //     System.out.println("Inserting vector: " + i);
                //     short[] vec = new short[100];
                //     for (int j = 0; j < 100; j++) {
                //         vec[j] = (short) random.nextInt(10000);
                //     }
                //     Vector100Dtype key = new Vector100Dtype(vec);
                //     RID rid = new RID(); // Fake RID for testing

                //     lshIndex.insert(key, rid);
                // }

                // // Step 3: Close and Flush to Disk
                // lshIndex.close();
                // System.out.println("\n✅ Database closed successfully.");

                // // Step 4: Reopen the Database and Verify Index Persistence
                // System.out.println("\n🔄 Reopening LSHFFile...");
                // LSHFFile reopenedIndex = new LSHFFile("lsh_test_index", 5, 3);

                // // Step 5: Ensure Indexes Are Retrieved
                // for (int i = 0; i < 3; i++) {
                //     String indexName = "BTree_layer" + i;
                //     PageId pageId = SystemDefs.JavabaseDB.get_file_entry(indexName);
                //     if (pageId != null) {
                //         System.out.println("✅ Index " + indexName + " successfully reloaded from disk.");
                //     } else {
                //         System.out.println("❌ Index " + indexName + " NOT found after reopening!");
                //     }
                // }


            SystemDefs sysdef = new SystemDefs("minibase.lshftest", 5000, 2000, "Clock");
            LSHFFile lshf = new LSHFFile("myDatabase", 5, 10);
            Random random = new Random(42);

            Vector100Dtype queryKey = null;

            // ✅ Step 1: Insert Sample Data
            for (int i = 0; i < 300; i++) {
                //System.out.println("Inserting vector: " + i);
                short[] vec = new short[100];
                for (int j = 0; j < 100; j++) {
                    vec[j] = (short) random.nextInt(10000);
                }
                Vector100Dtype key = new Vector100Dtype(vec);

                queryKey = new Vector100Dtype(vec);

                RID rid = new RID(); // Fake RID for testing

                Vector100DKey insert = new Vector100DKey(key);

                lshf.insert(insert, rid);

                
            }

            //✅ Step 2: Scan and Print Tree Structure
            System.out.println("🔍 Printing Initial BTree Structure...");
            LSHFFileScan scan = new LSHFFileScan(lshf.GetIndexLayer(1));  // Scanning first layer
            scan.LSHFFileScan();
            //scan.closeScan();

            lshf.LSHFFileScan();

            System.out.println();
            System.out.println("🔍 Finished printing structure...");

            System.out.println("Starting a NNSearch");

            lshf.NN_Search(queryKey, 100);

            System.out.println("Finishing a NNSearch");

            System.out.println("Starting a Range_Search");

            lshf.Range_Search(queryKey, 40000);

            System.out.println("Finishing a Range_Search");

            // ✅ Step 3: Close and Reopen Database
            System.out.println();
            lshf.close();
            System.out.println();
            System.out.println("✅ Database closed successfully.");

            // System.out.println("🔄 Reopening Database...");
            // LSHFFile newLshf = new LSHFFile("myDatabase", 3, 3);

            // // ✅ Step 4: Scan Again to Verify Persistence
            // System.out.println("🔍 Printing BTree Structure After Reopening...");
            // LSHFFileScan newScan = new LSHFFileScan(newLshf.GetIndexLayer(1));
            // newScan.printTreeStructure();
            // newScan.closeScan();

            System.out.println("✅ Test Completed!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}