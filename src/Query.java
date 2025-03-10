import java.io.*;
import diskmgr.PCounter;


public class Query {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Query <DBNAME> <QSNAME> <INDEXOPTION> <NUMBUF>");
            return;
        }

        String dbName = args[0];  // Database Name
        String qsName = args[1];  // Query Specification File
        boolean useLSH = args[2].equalsIgnoreCase("Y"); // Use LSH Index or not
        int numBuf = Integer.parseInt(args[3]);  // Number of buffer pages

        System.out.println("Starting Query Execution...");
        System.out.println("Database: " + dbName);
        System.out.println("Query File: " + qsName);
        System.out.println("Using LSH Index: " + (useLSH ? "Yes" : "No"));
        System.out.println("Buffer Pages: " + numBuf);

        try {
            // 🔹 Step 1: Read the query file
            BufferedReader reader = new BufferedReader(new FileReader(qsName));
            String queryLine = reader.readLine().trim(); // Read first query

            // 🔹 Step 2: Identify query type
            if (queryLine.startsWith("Range(")) {
                processRangeQuery(queryLine, dbName, useLSH);
            } else if (queryLine.startsWith("NN(")) {
                processNearestNeighborQuery(queryLine, dbName, useLSH);
            } else {
                System.out.println("Invalid query type in file: " + qsName);
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 🔹 Step 7: Output disk usage stats
        System.out.println("Disk pages read: " + PCounter.rcounter);
        System.out.println("Disk pages written: " + PCounter.wcounter);
    }

    private static void processRangeQuery(String queryLine, String dbName, boolean useLSH) {
        try {
            // 🔹 Extract parameters from "Range(QA, T, D, ...)"
            String[] parts = queryLine.replace("Range(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim()+".txt";
            int distanceThreshold = Integer.parseInt(parts[2].trim());

            // 🔹 Read the target vector from the file
            int[] targetVector = readVectorFromFile(targetVectorFile);

            System.out.println("Processing Range Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Distance Threshold: " + distanceThreshold);

            if (useLSH) {
                System.out.println("Using LSH-Forest for range query...");
                // TODO: Implement LSH-Forest range search
            } else {
                System.out.println("Performing full heapfile scan for range query...");
                // TODO: Implement full heapfile scan for range search
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processNearestNeighborQuery(String queryLine, String dbName, boolean useLSH) {
        try {
            // 🔹 Extract parameters from "NN(QA, T, K, ...)"
            String[] parts = queryLine.replace("NN(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim()+".txt";
            int k = Integer.parseInt(parts[2].trim());

            // 🔹 Read the target vector from the file
            int[] targetVector = readVectorFromFile(targetVectorFile);

            System.out.println("Processing Nearest Neighbor Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Number of Neighbors: " + k);

            if (useLSH) {
                System.out.println("Using LSH-Forest for nearest neighbor search...");
                // TODO: Implement LSH-Forest nearest neighbor search
            } else {
                System.out.println("Performing full heapfile scan for nearest neighbors...");
                // TODO: Implement full heapfile scan for nearest neighbors
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int[] readVectorFromFile(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String[] values = reader.readLine().trim().split("\\s+");
            reader.close();

            if (values.length != 100) {
                throw new IllegalArgumentException("Invalid target vector format: Must contain exactly 100 integers.");
            }

            int[] vector = new int[100];
            for (int i = 0; i < 100; i++) {
                vector[i] = Integer.parseInt(values[i]);
            }
            return vector;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}