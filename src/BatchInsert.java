import diskmgr.PCounter;
import global.AttrType;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.Heapfile;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;

public class BatchInsert {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java BatchInsert <h> <L> <DATAFILENAME> <DBNAME>");
            return;
        }

        int h = Integer.parseInt(args[0]); // Number of hash functions per layer
        int L = Integer.parseInt(args[1]); // Number of layers
        String dataFileName = args[2]; // Data file
        String dbName = args[3]; // Database name

        int numPages = 1000; // Disk pages allocated
        int bufferSize = 50; // Buffer pool size

        try {
            // 🔹 Step 1: Initialize MiniBase
            new SystemDefs(dbName, numPages, bufferSize, "Clock");

            // 🔹 Step 2: Read the input file
            BufferedReader reader = new BufferedReader(new FileReader(dataFileName));
            int numAttributes = Integer.parseInt(reader.readLine().trim()); // Read first line (number of attributes)
            String[] attrTypes = reader.readLine().trim().split("\\s+"); // Read second line (attribute types)

            // 🔹 Step 3: Define Schema
            AttrType[] schema = new AttrType[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                int typeCode = Integer.parseInt(attrTypes[i]);
                switch (typeCode) {
                    case 1: schema[i] = new AttrType(AttrType.attrInteger); break;
                    case 2: schema[i] = new AttrType(AttrType.attrReal); break;
                    case 3: schema[i] = new AttrType(AttrType.attrString); break;
                    case 4: schema[i] = new AttrType(AttrType.attrVector100D); break;
                    default: throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
                }
            }

            // 🔹 Step 4: Create Heapfile
            Heapfile heapfile = new Heapfile(dbName + "_heap");

            // 🔹 Step 5: Read and insert tuples
            String line;
            while ((line = reader.readLine()) != null) {
                Tuple tuple = new Tuple();
                tuple.setHdr((short) numAttributes, schema, getStringSizes(schema));

                int fieldNum = 1;
                for (int i = 0; i < numAttributes; i++) {
                    switch (schema[i].attrType) {
                        case AttrType.attrInteger:
                            System.out.println("Attr Integer: "+line.trim());
                            tuple.setIntFld(fieldNum, Integer.parseInt(line.trim()));

                            break;

                        case AttrType.attrReal:
                            System.out.println("Attr Real: "+line.trim());
                            tuple.setFloFld(fieldNum, Float.parseFloat(line.trim()));

                            break;

                        case AttrType.attrString:
                            System.out.println("Attr String: "+line.trim());
                            tuple.setStrFld(fieldNum, line.trim()); // Handles single or double-quoted names

                            break;

                        case AttrType.attrVector100D:
                            short[] vector = new short[100];
                            String[] vectorValues = line.trim().split("\\s+"); // Read 100 value
                             for (int j = 0; j < 100; j++) {
                                vector[j] =(short) Integer.parseInt(vectorValues[j]); //Change the value of short to int
                           }
                            tuple.set100DVectorFld(fieldNum, new Vector100Dtype(vector));
                            break;
                    }
                    fieldNum++;
                    if (i < numAttributes - 1) line = reader.readLine(); // Read next attribute
                }

                // Insert tuple into heap file
                heapfile.insertRecord(tuple.getTupleByteArray());
            }

            reader.close();
            System.out.println("Batch Insertion Complete!");

            // 🔹 Step 6: Output disk usage stats (added here!)
            System.out.println("Disk pages read: " + PCounter.rcounter);
            System.out.println("Disk pages written: " + PCounter.wcounter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Function to get string attribute sizes (required for setHdr)
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
