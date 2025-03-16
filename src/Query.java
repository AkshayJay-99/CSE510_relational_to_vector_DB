import diskmgr.PCounter;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import index.NNIndexScan;
import index.RSIndexScan;
import iterator.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Query <DBNAME> <QSNAME> <INDEXOPTION> <NUMBUF>");
            return;
        }
        int numPages = 1000; // Disk pages allocated
        String dbName = args[0];  // Database Name
        String qsName = args[1];  // Query Specification File
        boolean useLSH = args[2].equalsIgnoreCase("Y"); // Use LSH Index or not
        int numBuf = Integer.parseInt(args[3]);  // Number of buffer pages
        String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName; 
        FileScan sc_scan = null;

        System.out.println("Starting Query Execution...");
        System.out.println("Database: " + dbName);
        System.out.println("Query File: " + qsName);
        System.out.println("Using LSH Index: " + (useLSH ? "Yes" : "No"));
        System.out.println("Buffer Pages: " + numBuf);

        try {
            // 🔹 Step 1: Read the query file
            BufferedReader reader = new BufferedReader(new FileReader(qsName));
            String queryLine = reader.readLine().trim(); // Read first query

            SystemDefs sysdef = new SystemDefs( dbpath, 0, numBuf, "Clock" );
            Heapfile heapfile = new Heapfile("data_heap.in");

            FldSpec[] sc_projlist = new FldSpec[1];
            RelSpec sc_rel = new RelSpec(RelSpec.outer); 
            sc_projlist[0] = new FldSpec(sc_rel, 1);
            try {
                sc_scan = new FileScan("sc_heap.in", new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30}, (short) 1, 1, sc_projlist, null);
            }
            catch (Exception e) {
            e.printStackTrace();
            }
            Tuple t =new Tuple();
            try {
                t = sc_scan.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }            
            AttrType[] _schema = null;
            String schemaString = "";
            short _len_in1 = 0;
            if (t != null) {
                schemaString = t.getStrFld(1);
                _len_in1 = (short) (schemaString.length()-2);
                _schema = new AttrType[schemaString.length()-2];
                for (int i = 0; i < schemaString.length()-2; i++) {
                    int typeCode = schemaString.charAt(i) - '0';
                    switch (typeCode) {
                        case 1: _schema[i] = new AttrType(AttrType.attrInteger); break;
                        case 2: _schema[i] = new AttrType(AttrType.attrReal); break;
                        case 3: _schema[i] = new AttrType(AttrType.attrString); break;
                        case 4: _schema[i] = new AttrType(AttrType.attrVector100D); break;
                        default: throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
                    }
                }
            }
            System.out.println("schemaString: " + schemaString);
            int L = schemaString.charAt(schemaString.length()-1) - '0';
            int h = schemaString.charAt(schemaString.length()-2) - '0';
            System.out.println("H"+h);
            while (t != null) {
                try {
                    t = sc_scan.get_next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sc_scan.close();

            // 🔹 Step 2: Identify query type
            if (queryLine.startsWith("Range(")) {
                processRangeQuery(dbName, h, L, queryLine, useLSH, _schema, _len_in1);
            } else if (queryLine.startsWith("NN(")) {
                processNearestNeighborQuery(dbName, h, L, queryLine, useLSH, _schema, _len_in1);
            } else {
                System.out.println("Invalid query type in file: " + qsName);
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        flushPages();

        // 🔹 Step 7: Output disk usage stats
        System.out.println("Disk pages read: " + PCounter.rcounter);
        System.out.println("Disk pages written: " + PCounter.wcounter);
    }

    private static void processRangeQuery(String dbName, int h, int L, String queryLine, boolean useLSH, AttrType[] attrType, int attrSize) {
        try {
            // 🔹 Extract parameters from "Range(QA, T, D, ...)"

            String[] parts = queryLine.replace("Range(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim()+".txt";
            int distanceThreshold = Integer.parseInt(parts[2].trim());
            // 🔹 Read the target vector from the file
            Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);
            int noOutFlds = parts.length - 3;
            FldSpec[] projlist = new FldSpec[noOutFlds];
            RelSpec rel = new RelSpec(RelSpec.outer); 
            for (int i = 0; i < noOutFlds; i++) {
                projlist[i] = new FldSpec(rel, Integer.parseInt(parts[3+i].trim()));
            }
            System.out.println("Processing Range Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Distance Threshold: " + distanceThreshold);


            List<Tuple> results = new ArrayList<>();
            AttrType[] out_types = new AttrType[noOutFlds];
            RSIndexScan rs = null;
            if (useLSH) {
                System.out.println("Using LSH-Forest for range query...");
            try {
                rs = new RSIndexScan(new IndexType(IndexType.LSHF_Index), "data_heap.in", dbName+'_'+queryField+'_'+h+'_'+L, attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, distanceThreshold);
            } catch (Exception e) {
                e.printStackTrace();
            }              
            } 
            else {
                System.out.println("Performing full heapfile scan for range query...");
                TupleOrder[] order = new TupleOrder[2];
                order[0] = new TupleOrder(TupleOrder.Ascending);
                order[1] = new TupleOrder(TupleOrder.Descending);
                try {
                    rs = new RSIndexScan(new IndexType(IndexType.None), "data_heap.in", "", attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, distanceThreshold);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            results = rs.get_all_results();

            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = attrType[projlist[i].offset - 1];
                    }
                result.print(out_types);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processNearestNeighborQuery(String dbName, int h, int L, String queryLine, boolean useLSH, AttrType[] attrType, short attrSize) {
        try {
            // 🔹 Extract parameters from "NN(QA, T, K, ...)"
            String[] parts = queryLine.replace("NN(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim()+".txt";
            int k = Integer.parseInt(parts[2].trim());
            // 🔹 Read the target vector from the file
            Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);
            int noOutFlds = parts.length - 3;
            FldSpec[] projlist = new FldSpec[noOutFlds];
            RelSpec rel = new RelSpec(RelSpec.outer); 
            for (int i = 0; i < noOutFlds; i++) {
                projlist[i] = new FldSpec(rel, Integer.parseInt(parts[3+i].trim()));
            }
            System.out.println("Processing Nearest Neighbor Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Number of Neighbors: " + k);

            List<Tuple> results = new ArrayList<>();
            AttrType[] out_types = new AttrType[noOutFlds];
            NNIndexScan nn = null;
            if (useLSH) {
                System.out.println("Using LSH-Forest for nearest neighbor search...");
                try {
                    nn = new NNIndexScan(new IndexType(IndexType.LSHF_Index), "data_heap.in", dbName+'_'+queryField+'_'+h+'_'+L, attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, k);
                } 
                catch (Exception e) {
                    e.printStackTrace();
                }              
            } 
            else {
                System.out.println("Performing full heapfile scan for nearest neighbors...");
                System.out.println("Performing full heapfile scan for range query...");
                TupleOrder[] order = new TupleOrder[2];
                order[0] = new TupleOrder(TupleOrder.Ascending);
                order[1] = new TupleOrder(TupleOrder.Descending);
                try {
                    nn = new NNIndexScan(new IndexType(IndexType.None), "data_heap.in", "", attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, k);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            results = nn.get_all_results();

            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = attrType[projlist[i].offset - 1];
                    }
                result.print(out_types);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private static short[] getStringSizes(AttrType[] schema) {
        int count = 0;
        for (AttrType attr : schema) {
            if (attr.attrType == AttrType.attrString) count++;
        }
        short[] strSizes = new short[count];
        Arrays.fill(strSizes, (short) 30); // Default string length
        return strSizes;
    }

    private static Vector100Dtype readVectorFromFile(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String[] values = reader.readLine().trim().split("\\s+");
            reader.close();

            if (values.length != 100) {
                throw new IllegalArgumentException("Invalid target vector format: Must contain exactly 100 integers.");
            }

            short[] vector = new short[100];
            for (int i = 0; i < 100; i++) {
                vector[i] = Short.parseShort(values[i]);
            }
            Vector100Dtype target = new Vector100Dtype(vector);

            return target;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}