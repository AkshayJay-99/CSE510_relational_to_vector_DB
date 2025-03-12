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

            // Step 1: Initialize MiniBase & Create LSH Index
            SystemDefs sysdef = new SystemDefs("minibase.lshftest", 5000, 2000, "Clock");
            LSHFFile lshIndex = new LSHFFile("lsh_test_index", 5, 3); // h=5, L=3
            Random random = new Random();
            short[] queryVec = new short[100];
            // Step 2: Insert Sample Vectors
            //System.out.println("Inserting Vectors");
            for (int i = 0; i < 50; i++) {
                short[] vec = new short[100];
                //Arrays.fill(vec, (short) (i * 10)); // Simple increasing pattern
                for(int j = 0; j < 100; j++)
                {
                    vec[j] = (short) random.nextInt(10000);
                }
                Vector100Dtype key = new Vector100Dtype(vec);
                RID rid = new RID(); // Fake RID for testing
                //System.out.println("Creating fresh RID: Page " + rid.pageNo.pid + ", Slot " + rid.slotNo);
                queryVec = vec;
                lshIndex.insert(key, rid);
                System.out.println("                                                Inserted vector " + (i));
            }

            // Step 3: Run Nearest Neighbor Search
            //short[] queryVec = new short[100];
            Vector100Dtype queryKey = new Vector100Dtype(queryVec);

            List<RID> nearest = lshIndex.NN_Search(queryKey, 16);
            System.out.println("\nTop-5 Nearest Neighbors:");
            for (RID rid : nearest) {
                System.out.println("RID: Page " + rid.pageNo.pid + ", Slot " + rid.slotNo);
            }


             //System.out.println("Printing query array: " + Arrays.toString(queryKey.getValues()));
            // Step 4: Run Range Search
            List<RID> rangeResults = lshIndex.rangeSearch(queryKey, 1000);
            System.out.println("\nRange Search Results (Distance <= 50):");
            for (RID rid : rangeResults) {
                System.out.println("RID: Page " + rid.pageNo.pid + ", Slot " + rid.slotNo);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}