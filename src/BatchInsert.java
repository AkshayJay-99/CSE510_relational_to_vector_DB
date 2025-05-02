import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import lshfindex.*;
import java.util.ArrayList;
import btree.*;

public class BatchInsert {
    public static void main(String[] args) {
        if (args.length != 4 && args.length != 2) {
            System.out.println("Usage: java BatchInsert <h> <L> <DATAFILENAME> <DBNAME>");
            return;
        }

        if (args.length == 4)
        {
            int h = Integer.parseInt(args[0]); // Number of hash functions per layer
            int L = Integer.parseInt(args[1]); // Number of layers
            String dataFileName = args[2]; // Data file
            String dbName = args[3]; // Database name
            String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName; 
            int numPages = 12000; // Disk pages allocated
            int bufferSize = 16000; // Buffer pool size
            
            try {
                // 🔹 Step 1: Initialize MiniBase
                SystemDefs sysdef = new SystemDefs(dbpath, numPages, bufferSize, "Clock");


                // 🔹 Step 2: Read the input file
                BufferedReader reader = new BufferedReader(new FileReader(dataFileName));
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
                Heapfile heapfile_sc = new Heapfile("sc_heap.in");
                Tuple sc_tuple = new Tuple();
                sc_tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                //System.out.println("Values within attr_char: " + attr_char);
                sc_tuple.setStrFld(1, attr_char);  
                RID sc_rid = heapfile_sc.insertRecord(sc_tuple.getTupleByteArray());
                System.out.println("Schema stored with RID: Page " + sc_rid.pageNo.pid + ", Slot " + sc_rid.slotNo);

                // 🔹 Step 4: Create Heapfile
                //Heapfile heapfile = new Heapfile("data_heap.in");
                System.out.println("Printing data file: " + dataFileName.substring(0, dataFileName.length() - 4));
                String dataFileName_ = dataFileName.substring(0, dataFileName.length() - 4);

                //Heapfile heapfile = new Heapfile("data_heap.in");
                Heapfile heapfile = new Heapfile("TBD_PageName" + ".in");

                PageId pid = SystemDefs.JavabaseDB.get_file_entry("TBD_PageName.in");

                System.out.println("we found the file at: " + pid);


                // 🔹 Step 5a: Create LSH-Forest Index Placeholder
                LSHFFile[] lshf = new LSHFFile[numAttributes];
                for (int i = 0; i < numAttributes; i++) {
                    if (schema[i].attrType == AttrType.attrVector100D) {
                        lshf[i] = new LSHFFile(dbName+'_'+(i+1)+'_'+h+'_'+L, h, L);
                    }
                }

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
                    //System.out.println(tuple.get100DVectorFld(2));
                    RID recordID =  heapfile.insertRecord(tuple.getTupleByteArray());
                    for (int i = 0; i < numAttributes; i++) {
                        if (schema[i].attrType == AttrType.attrVector100D) {
                            Vector100Dtype vector100D = new Vector100Dtype();
                            vector100D = tuple.get100DVectorFld(i + 1);
                            lshf[i].insert(new Vector100DKey(vector100D), recordID);
                            //System.out.println("Inserted into LSHF: Page " + recordID.pageNo.pid + ", Slot " + recordID.slotNo + " for attribute " + (i + 1+" with vector: ")); // + vector100D
                        }
                    }  
                    


                    //System.out.println("Record ID: Page " + recordID.pageNo.pid + ", Slot " + recordID.slotNo);
                    //verifyInsertion(dbName,schema);
                    // 🔹 Insert ALL vectors from the tuple into LSH-Forest
                    // for (short[] vector : vectorList) {
                    //     insertIntoLSH(lshIndex, vector, recordID);
                    // }
                //System.out.println("Creating fresh RID: Page " + recordID.pageNo.pid + ", Slot " + recordID.slotNo);

                }
                for (int i = 0; i < numAttributes; i++) {
                    if (schema[i].attrType == AttrType.attrVector100D) {
                        lshf[i].close();
                        //saveLSHIndex(lshf[i]);
                    }
                }
                reader.close();
                //System.out.println("Batch Insertion Complete!");
                flushPages();
                
                // Optionally, here's how you could shut down the system entirely, which also ensures data is saved
                SystemDefs.JavabaseDB.closeDB();  // Close the database
                // 🔹 Step 6: Output disk usage stats (added here!)
                System.out.println("Disk pages read: " + PCounter.rcounter);
                System.out.println("Disk pages written: " + PCounter.wcounter);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        else                    // -----------------------------------------New insert----------------------------------------------
        if(args.length == 2)
        {
            //int h = Integer.parseInt(args[0]); // Number of hash functions per layer
            //int L = Integer.parseInt(args[1]); // Number of layers

            String dataFileName = args[0]; // Data file
            //String dbName = args[0]; // Database name
            String dbName = "myTester";
            String relName = args[1];
            String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName; 
            int numPages = 12000; // Disk pages allocated
            int bufferSize = 16000; // Buffer pool size

            BatchInsert(dbName, dataFileName, relName);
            
            
        }

        
    }


    public static void BatchInsert(String dbName, String dataFileName, String relName)
    {
        try {
            // 🔹 Step 1: Initialize MiniBase
            int bufferSize = 16000;
            //String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName; 
            //SystemDefs sysdef = new SystemDefs(dbpath, 0, bufferSize, "Clock");
            
            //System.out.println("Name of the relName: " + relName + ".in");
            //System.out.println("Name of the relName stored: " + "TBD_PageName.in");

            PageId pid = SystemDefs.JavabaseDB.get_file_entry(relName+".in");

            System.out.println("Did we did we find anything for the pid: " + pid);

            Heapfile heapfile = new Heapfile(relName+".in");


            // 🔹 Step 2: Read the input file
            BufferedReader reader = new BufferedReader(new FileReader(dataFileName));
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
            attr_char +=(char) (2+ '0');
            attr_char +=(char) (1+ '0');
            System.out.println(attr_char);
            Heapfile heapfile_sc = new Heapfile("sc_heap.in");

            Tuple sc_tuple = new Tuple();
            sc_tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
            sc_tuple.setStrFld(1, attr_char);  
            RID sc_rid = heapfile_sc.insertRecord(sc_tuple.getTupleByteArray());
            System.out.println("Schema stored with RID: Page " + sc_rid.pageNo.pid + ", Slot " + sc_rid.slotNo);
            

            // 🔹 Step 4: Create Heapfile
            //dataFileName

            // System.out.println("Name of the relName: " + relName + ".in");
            // System.out.println("Name of the relName stored: " + "TBD_PageName.in");
            // PageId pid = SystemDefs.JavabaseDB.get_file_entry("TBD_PageName.in");

            // System.out.println("Did we did we find anything for the pid: " + pid);

            // Heapfile heapfile = new Heapfile("TBD_PageName" + ".in");

            
            //Heapfile heapfile = new Heapfile("data_heap.in");


            // 🔹 Step 5a: Create LSH-Forest Index Placeholder
            // LSHFFile[] lshf = new LSHFFile[numAttributes];
            // for (int i = 0; i < numAttributes; i++) {
            //     if (schema[i].attrType == AttrType.attrVector100D) {
            //         lshf[i] = new LSHFFile(dbName+'_'+(i+1)+'_'+2+'_'+1, 2, 1); 
            //         //lshf[i] = new LSHFFile(dbName+'_'+(i+1)+'_'+h+'_'+L, h, L);               // Need to read these file in to get the L/h values
            //     }
            // }

            short[] vec = new short[100];
            for (int j = 0; j < 100; j++) {
                vec[j] = (short) 100;
            }

            Vector100Dtype queryKey = new Vector100Dtype(vec);
            
            // if(lshf[1] == null)
            //     System.out.println("We got nothing");
            // else
            // {
            //     System.out.println("We got something");
            //     lshf[1].NN_Search(queryKey, 1000000);
            // }
                
            
            //.NN_Search(queryKey, 1000000);

            // 🔹 Step 5b: Read and insert tuples

            int numRecordsInserted = 0;
            int num100DRecordInserted = 0;
            String line;
            
            Scan indexscan;
            Tuple indextuple;
            RID indexrid;

            System.out.println("retrieving values within: " + (relName + "indexes"));
            PageId check_pid = SystemDefs.JavabaseDB.get_file_entry(relName + "indexes");

            System.out.println("Did we did we find anything for the pid: " + check_pid);

            Heapfile heapfileIndex = new Heapfile(relName + "indexes");

            indexscan = heapfileIndex.openScan();
            indexrid = new RID();

            ArrayList<String> potential_indexes = new ArrayList<>();

            while ((indextuple = indexscan.getNext(indexrid)) != null) {
                indextuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                String potential_match = indextuple.getStrFld(1);
                potential_indexes.add(potential_match);
                System.out.println("potential val: "+potential_match);
            }

            indexscan.closescan();



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
                //System.out.println(tuple.get100DVectorFld(2));
                numRecordsInserted ++;
                RID recordID =  heapfile.insertRecord(tuple.getTupleByteArray());
                for(int j = 0; j < potential_indexes.size(); j++)
                {

                    String[] values = potential_indexes.get(j).split("_");
                    
                    int columToCheck = Integer.parseInt(values[1]);
                        switch (schema[columToCheck].attrType) 
                        {
                        // case AttrType.attrInteger:

                        //     break;
                        case AttrType.attrReal:
                            float  floVla = tuple.getFloFld(columToCheck+1);
                            //System.out.println("Checking for a realNum attr: " + floVla);
                            String btreeName = relName + "_" + Integer.parseInt(values[1]);
                            BTreeFile btree = new BTreeFile(btreeName);
                            btree.IntegerKey key = new btree.IntegerKey((int) floVla);
                            btree.insert(key, recordID);
                            btree.close();
                            break;
                        // case AttrType.attrString:

                        //     break;
                        case AttrType.attrVector100D:
                            Vector100Dtype vecVal = tuple.get100DVectorFld(columToCheck+1);
                            //System.out.println("Checking for a 100DVector");
                            int column = Integer.parseInt(values[1]);
                            int hashes = Integer.parseInt(values[2]);
                            int layers = Integer.parseInt(values[3]);

                            LSHFFile lshf = new LSHFFile(dbName+'_'+relName+'_'+(column)+'_'+hashes+'_'+layers, hashes, layers); 
                            Vector100DKey insert = new Vector100DKey(vecVal);
                            lshf.insert(insert, recordID);
                            lshf.close();
                            break;
                        }
                }
                // for (int i = 0; i < numAttributes; i++) {
                //     if (schema[i].attrType == AttrType.attrVector100D) {
                //         Vector100Dtype vector100D = new Vector100Dtype();
                //         vector100D = tuple.get100DVectorFld(i + 1);
                //         //lshf[i].insert(new Vector100DKey(vector100D), recordID);
                //         num100DRecordInserted ++;
                //         //System.out.println("Inserted into LSHF: Page " + recordID.pageNo.pid + ", Slot " + recordID.slotNo + " for attribute " + (i + 1+" with vector: " + vector100D));
                //     }
                // }  
                //System.out.println("Record ID: Page " + recordID.pageNo.pid + ", Slot " + recordID.slotNo);
                //verifyInsertion(dbName,schema);
                // 🔹 Insert ALL vectors from the tuple into LSH-Forest
                // for (short[] vector : vectorList) {
                //     insertIntoLSH(lshIndex, vector, recordID);
                // }
                //System.out.println("Creating fresh RID: Page " + recordID.pageNo.pid + ", Slot " + recordID.slotNo);

            }

            //System.out.println("numRecordsInserted: " + numRecordsInserted + " num100DRecordInserted: " + num100DRecordInserted);
            // for (int i = 0; i < numAttributes; i++) {
            //     if (schema[i].attrType == AttrType.attrVector100D) {
            //         lshf[i].close();
            //         //saveLSHIndex(lshf[i]);
            //     }
            // }
            reader.close();
            System.out.println("Batch Insertion Complete!");
            flushPages();
            
            // Optionally, here's how you could shut down the system entirely, which also ensures data is saved
            //SystemDefs.JavabaseDB.closeDB();  // Close the database
            // 🔹 Step 6: Output disk usage stats (added here!)
            System.out.println("Disk pages read: " + PCounter.rcounter);
            System.out.println("Disk pages written: " + PCounter.wcounter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // private static void insertIntoLSH(LSHFFile lshIndex, short[] vector, RID rid) {
    //     // TODO: Implement real LSH insertion logic
    //     System.out.println("Inserting into LSH: " + Arrays.toString(vector));
    //     lshIndex.insert(new Vector100DKey(vector), rid);
    // }

    // // 🔹 Placeholder Function to Save LSH Index
    // private static void saveLSHIndex(LSHFFile lshIndex) {
    //     // TODO: Implement real LSH saving logic
    //     System.out.println("Saving LSH Index...");
    //     lshIndex.saveIndex(); // Assuming there's a saveIndex method
    // }

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


    public static void flushPages() {
        try {
            SystemDefs.JavabaseBM.flushAllPages(); // Assuming there's a method to flush all pages
            System.out.println("All pages flushed to disk.");
        } catch (Exception e) {
            System.err.println("Error flushing pages: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void verifyInsertion(String dbName, AttrType[] schema) {
        try {
            Heapfile heapfile = new Heapfile("data_heap.in");
            Scan scan = heapfile.openScan();
            Tuple tuple;
            RID rid = new RID();

            System.out.println("\n🔹 Verifying Data Insertion: Scanning Heapfile...");
            int count = 0;

            while ((tuple = scan.getNext(rid)) != null) {
                tuple.setHdr((short) schema.length, schema, getStringSizes(schema)); // Set header before printing
                //tuple.print(schema); // ✅ Corrected print statement
                count++;
            }

            scan.closescan();
            System.out.println("✅ Total Tuples Inserted: " + count);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
