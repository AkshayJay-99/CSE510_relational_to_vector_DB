

package lshfindex;

import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;
import java.io.*;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Random;
import java.util.stream.Collectors;

public class LSHFFile extends IndexFile
{
    private int layers;
    private int hashes;

    private LSHFHeaderPage headerPage;
    private Heapfile heapFile;
    private BufMgr bufferManager;

    private int width = 150;
    private Random random = new Random();

    private double[][][] projections;
    private int[][] b_values;

    int buckets = 0;

    private LSHFBTreeFile[] btreeIndex;

    // public LSHFFile(int _layers, int _hashes)
    // {
    //     this.layers = _layers;
    //     this.hashes = _hashes;

    //     heapFile = new Heapfile(fileName);
    //     headerPage = new LSHFHeaderPage(new PageId());
    //     headerPage.setNumLayers(_layers);
    //     headerPage.setNumHashes(_hashes);
    // }

    public void initalizeHashFunction()
    {
        projections = new double[layers][hashes][100];
        b_values = new int[layers][hashes];

        Random random = new Random();
        

        for(int i = 0; i < layers; i++)
        {
            for(int j = 0; j < hashes; j++)
            {
                // Create a projection for each hash (Need to store these projections for each hash)
                // Create b values, should be random uniform values; b = random.uniform(0, width)
                b_values[i][j] = random.nextInt(width);

                for(int k = 0; k < 100; k++)
                {
                    projections[i][j][k] = random.nextGaussian(); 
                }

                //int randomNumber = random.nextInt(100);
                //Math.floor(a)
            }
        }
    }

    public void saveHashFunctions() throws Exception 
    {
        Heapfile hashFile = new Heapfile("hash_functions");
        Tuple tuple = new Tuple();

        AttrType[] attrTypes = new AttrType[]{ 
        new AttrType(AttrType.attrInteger), // Layer ID
        new AttrType(AttrType.attrInteger), // Hash ID
        new AttrType(AttrType.attrString),   // Projection Vector (100D)
        new AttrType(AttrType.attrInteger)  // b_value (int)
        };

        short[] strSizes = new short[]{800};
        tuple.setHdr((short) 4, attrTypes, strSizes);

        for (int i = 0; i < layers; i++) {
            for (int j = 0; j < hashes; j++) {
                tuple.setIntFld(1, i);  // Layer ID
                tuple.setIntFld(2, j);  // Hash ID

                // Convert projection vector to byte array
                String projString = Arrays.stream(projections[i][j]).mapToObj(Double::toString).collect(Collectors.joining(","));
                tuple.setStrFld(3, projString);

                tuple.setIntFld(4, b_values[i][j]); // Store b_value

                hashFile.insertRecord(tuple.getTupleByteArray());
            }
        }
    }

    public void loadHashFunctions() throws Exception {
        Heapfile hashFile = new Heapfile("hash_functions");
        Scan scan = new Scan(hashFile);
        Tuple tuple;
        RID rid = new RID();

        projections = new double[layers][hashes][100];
        b_values = new int[layers][hashes];

        while ((tuple = scan.getNext(rid)) != null) {
            tuple.setHdr((short) 4, 
                new AttrType[]{
                    new AttrType(AttrType.attrInteger), 
                    new AttrType(AttrType.attrInteger), 
                    new AttrType(AttrType.attrString), 
                    new AttrType(AttrType.attrInteger)
                }, 
                new short[]{800}); // Same size assumption

            int layer = tuple.getIntFld(1);
            int hashId = tuple.getIntFld(2);

            // Retrieve projection vector from stored string
            String projString = tuple.getStrFld(3);
            double[] projArray = Arrays.stream(projString.split(","))
                                    .mapToDouble(Double::parseDouble)
                                    .toArray();

            projections[layer][hashId] = projArray;
            b_values[layer][hashId] = tuple.getIntFld(4);
        }

    }

    public double[] convertShortToDouble(short[] inputArray)
    {
        double[] convertedArray = new double[inputArray.length];
        for(int i = 0; i < inputArray.length; i++)
            convertedArray[i] = (double) inputArray[i];
        return convertedArray;
    }

    public void close() {
        try {
            //System.out.println("🔍 Checking file entries BEFORE closing...");
            for (int i = 0; i < btreeIndex.length; i++) {
                if (btreeIndex[i] != null) {
                    String indexName = "BTree_layer" + i;
                    PageId pageId = SystemDefs.JavabaseDB.get_file_entry(indexName);
                    
                    // if (pageId == null) {
                    //     System.out.println("❌ Index file entry MISSING before closing: " + indexName);
                    // } else {
                    //     System.out.println("✅ Found existing index file entry: " + indexName + " (PageId: " + pageId.pid + ")");
                    // }

                    //System.out.println("Attempting to close index: " + i);
                    btreeIndex[i].close();
                    //System.out.println("Successfully closed index: " + i);
                }
            }

            // ✅ Flush all pages to disk to ensure persistence
            //System.out.println("✅ Flushing all pages to disk...");
            SystemDefs.JavabaseBM.flushAllPages();
           // System.out.println("✅ All pages flushed to disk.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void verifyIndexFiles() {
        for (int i = 0; i < btreeIndex.length; i++) {
            String indexFile = "BTree_Layer" + i;
            File file = new File(indexFile);
            if (file.exists()) {
                System.out.println("✅ Index file exists: " + indexFile);
            } else {
                System.out.println("❌ Index file MISSING: " + indexFile);
            }
        }
    }


    public void insert(Vector100DKey key, RID rid) throws  Exception, SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {
        double[] vector = convertShortToDouble(key.getKey().getValues());
        //System.out.println("We made it to the insert method");
        //System.out.flush();
        for(int i = 0; i < layers; i++)
        {
            //System.out.println("Starting on layer: " + i);
            int[] hash_list = new int[hashes];
            
            int layer = i;

            String bucketKey = "layer" + i;

            //btreeIndex[layer] = new LSHFBTreeFile("BTree_layer" + layer, AttrType.attrString, 250, 1);
            // if (btreeIndex[layer] == null) {
            //     //System.out.println("✅ Creating new BTree index for layer " + layer);
            //     btreeIndex[layer] = new LSHFBTreeFile("BTree_layer" + layer, AttrType.attrString, 250, 1);
            // }
            


            String indexName = "BTree_layer" + layer;

            // PageId pageId = get_file_entry(indexFile);
            // if (pageId == null) {
            //     System.out.println("❌ Index file entry MISSING: " + indexFile);
            // } else {
            //     System.out.println("✅ Index file entry FOUND: " + indexFile + " -> PageId: " + pageId);
            // }


            if (btreeIndex[layer] == null) {

                 //System.out.println("⚠️ No existing BTree index found. Creating new one for " + indexName);

                PageId pageId = SystemDefs.JavabaseDB.get_file_entry(indexName);  // ✅ Ensure this runs first

                if (pageId == null) {
                    //System.out.println("❌ No file entry found for: " + indexName);
                    //System.out.println("📌 Creating new BTree index for: " + indexName);
                    btreeIndex[layer] = new LSHFBTreeFile(indexName, AttrType.attrString, 250, 1);
                } else {
                    //System.out.println("✅ Found existing file entry for: " + indexName + " -> PageId: " + pageId.pid);
                    btreeIndex[layer] = new LSHFBTreeFile(indexName);
                }

                // try {
                //     // Try to open an existing index first
                //     btreeIndex[layer] = new LSHFBTreeFile(indexName);
                //     System.out.println("✅ Loaded existing BTree index for layer " + layer);
                // } catch (Exception e) {
                //     // If it doesn't exist, create a new one
                //     System.out.println("⚠️ No existing BTree index found. Creating new one for layer " + layer);
                //     btreeIndex[layer] = new LSHFBTreeFile(indexName, AttrType.attrString, 250, 1);
                // }
            }
            

            for(int j = 0; j < hashes; j++)
            {
                //int layer = i;
                int hashVal = hash_function(projections[i][j], vector, b_values[i][j], width);
                hash_list[j] = hashVal;
                bucketKey += ("_" + hashVal);
                //System.out.println("We are hashing into the " + hashVal + " In layer: " + i);
                //storeInHeapFile(hashVal, layer, key, rid);
                
                
                btreeIndex[layer].insert( new StringKey(bucketKey), rid);

                // if (j < hashes - 1) {
                //     System.out.println("🛠 Storing Internal Key in Internal Node: " + bucketKey);
                //     childPage = btreeIndex[layer].insertInternal(new StringKey(bucketKey), currentNodePage);
                //     currentNodePage = childPage;
                //  }
            }

            //System.out.println("new bucket key: " + bucketKey);

            //Vector100DKey vectorKey = new Vector100DKey(key);

            //System.out.println("📌 Inserting Leaf Node: " + vectorKey + " | RID -> Page: " + rid.pageNo.pid + ", Slot: " + rid.slotNo);

            // System.out.println();
            // System.out.println();
            // System.out.println("Inserting leaf now");
            btreeIndex[layer].insertLeaf(key, rid, bucketKey);
            // System.out.println("Done inserting leaf");
            // System.out.println();
            // System.out.println();

        }

        //printBTreeStructure(0);
       
    }

    public LSHFBTreeFile GetIndexLayer(int layer)
    {
        return btreeIndex[layer];
    }




    // public void printBTreeStructure(int layer) {
    //     if (btreeIndex[layer] == null) {
    //         System.out.println("⚠️ BTree for layer " + layer + " is not initialized.");
    //         return;
    //     }

    //     try {
    //         System.out.println("\n🔍 **BTree Structure for Layer " + layer + "**");

    //         // Start scanning from the beginning
    //         LSHFBTFileScan scan = btreeIndex[layer].new_scan(null, null);
    //         KeyDataEntry entry;

    //         while ((entry = scan.get_next()) != null) {
    //             if (entry.key instanceof StringKey) {
    //                 String key = ((StringKey) entry.key).getKey();

    //                 if (entry.data instanceof LeafData) {
    //                     RID rid = ((LeafData) entry.data).getData();
    //                     System.out.println("📌 Leaf Node | Key: " + key + " | RID -> Page: " + rid.pageNo.pid + ", Slot: " + rid.slotNo);
    //                 } 
    //                 else if (entry.data instanceof IndexData) {
    //                     PageId pageId = ((IndexData) entry.data).getData();
    //                     System.out.println("🗂️ Internal Node | Key: " + key + " | Points to Page: " + pageId.pid);
    //                 } 
    //                 else {
    //                     System.out.println("⚠️ Unknown Node Type for Key: " + key + " | Data: " + entry.data.getClass().getSimpleName());
    //                 }
    //             }
    //         }

    //         scan.DestroyLSHFBTreeFileScan();
    //         System.out.println("✅ End of BTree structure for layer " + layer + "\n");
    //     } catch (Exception e) {
    //         System.out.println("❌ Error printing BTree structure for layer " + layer + ": " + e.getMessage());
    //     }
    // }






    private void storeInHeapFile(int hashVal, int layerVal, Vector100Dtype key, RID rid) throws  Exception, SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {
        // System.out.println("We are storing values in the heap file");
        // System.out.flush();
        Tuple t = new Tuple();

        AttrType[] attrTypes = new AttrType[]{
            new AttrType(AttrType.attrVector100D),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger)
        };

        short[] strSize = new short[0];

        t.setHdr((short) 3, attrTypes, strSize);
        t.set100DVectorFld(1, key);
        t.setIntFld(2, rid.pageNo.pid);
        t.setIntFld(3, rid.slotNo);
        // System.out.println("Tuple Schema: ");
        // System.out.println("Vector Field (1): " + key);
        // System.out.println("RID Page ID (2): " + rid.pageNo.pid);
        // System.out.println("RID Slot Number (3): " + rid.slotNo);
        // System.out.flush();

        // Using layerVal * 100000 to incorperate each layers value into the hashMap and prevents multiple layers from using the same bucket.
        
        int bucketKey = (layerVal * 100000) + hashVal; 
        PageId pageId;
        Heapfile bucketFile;
        if(headerPage.getHashBucketTable().containsKey(bucketKey))
        {
            //System.out.println("We found a previous bucket!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.flush();
            pageId = headerPage.getHashBucketTable().get(bucketKey);
            //System.out.println("pageId + "+pageId);
            //System.out.println("pageId.pid + "+pageId.pid);
            //bucketFile = new Heapfile("bucket_" + pageId.pid + "_" + layerVal);
            System.out.flush();
        }
        else
        {
            buckets ++;
            //System.out.println("We are making a new bucket??????????????????????????????????????????????????????????????????????????????????????????" + buckets);
            pageId = new PageId(SystemDefs.JavabaseBM.newPage(new Page(), 1).pid);
            //headerPage.getHashBucketTable().put(bucketKey, pageId);
            //System.out.println("pageId + "+pageId);
            //System.out.println("pageId.pid + "+pageId.pid);
            
        }
        // System.out.println("Printing bucket name: bucket_" + pageId.pid + "_" + layerVal);
        // System.out.println("Printing bucket name with hash instead: bucket_" + pageId.pid + "_" + hashVal);
        //Heapfile bucketFile = new Heapfile("bucket_" + pageId.pid);

       
        // for (Map.Entry<Integer, PageId> entry : headerPage.getHashBucketTable().entrySet()) {
        //     System.out.println("Hash Value: " + entry.getKey() + " -> Page ID: " + entry.getValue().pid);
        // }
        //System.out.println("Heap file record count before insert: " + bucketFile.getRecCnt());

        // System.out.println("🛠️ Debugging Tuple Serialization...");
        // System.out.println("📏 Tuple Actual Size: " + t.getLength());
        // System.out.println("📏 Serialized Byte Array Size: " + t.getTupleByteArray().length);
        //System.out.println("Tuple Size: " + t.size());
        
        // System.out.println("input RID -> Page: " + rid.pageNo.pid + " | Slot: " + rid.slotNo);
        // System.out.println("Tuple being inserted -> Vector: " + Arrays.toString(t.get100DVectorFld(1).getValues()) + 
        //            " | RID Page: " + t.getIntFld(2) + 
        //            " | RID Slot: " + t.getIntFld(3));
        bucketFile = new Heapfile("bucket_" + pageId.pid + "_" + layerVal);

        RID newRid = new RID();
        newRid = bucketFile.insertRecord(t.getTupleByteArray());

        rid.pageNo.pid = newRid.pageNo.pid;
        rid.slotNo = newRid.slotNo;

        
        Tuple updatedTuple = bucketFile.getRecord(newRid);
        updatedTuple.setHdr((short) 3, attrTypes, strSize);

        
        updatedTuple.setIntFld(2, newRid.pageNo.pid);
        updatedTuple.setIntFld(3, newRid.slotNo);

        
        bucketFile.updateRecord(newRid, updatedTuple);


        // Everything below this is for testing
        
        //System.out.println("Heap file record count before insert: " + bucketFile.getRecCnt());

        //System.out.println("Inserted RID -> Page: " + newRid.pageNo.pid + " | Slot: " + newRid.slotNo + " | just RID val: " + newRid);
        

        // for (Map.Entry<Integer, PageId> entry : headerPage.getHashBucketTable().entrySet()) {
        //     System.out.println("Hash Value: " + entry.getKey() + " -> Page ID: " + entry.getValue().pid);
        // }

       if (newRid == null || newRid.pageNo.pid < 0 || newRid.slotNo < 0) {
            System.out.println(" ERROR: insertRecord() returned invalid RID -> Page: " + 
                            (newRid == null ? "NULL" : newRid.pageNo.pid) + 
                            " | Slot: " + (newRid == null ? "NULL" : newRid.slotNo));
            return;
        }
        // Print updated record count AFTER inserting
        // int recordCountAfter = bucketFile.getRecCnt();
        // System.out.println("Heap file record count after insert: " + recordCountAfter);

       

        Tuple checkTuple = new Tuple();
        checkTuple.tupleInit(t.getTupleByteArray(), 0, t.size());
        checkTuple.setHdr((short) 3, attrTypes, strSize);


        Scan verifyScan = bucketFile.openScan();
        RID verifyRid = new RID();
        Tuple verifyTuple;
        
        while ((verifyTuple = verifyScan.getNext(verifyRid)) != null) {

            verifyTuple.setHdr((short) 3, attrTypes, strSize);
            if ( verifyRid.pageNo.pid < 0 || verifyRid.slotNo < 0) {
                System.out.println(" ERROR: Retrieved invalid RID from heap file -> Page: " + verifyRid.pageNo.pid + " | Slot: " + verifyRid.slotNo);
                continue;
            }

            // System.out.println("Scanned Tuple -> Page: " + verifyRid.pageNo.pid + ", Slot: " + verifyRid.slotNo);
            // System.out.println("Stored Vector: " + verifyTuple.get100DVectorFld(1));
            // System.out.println("Stored RID Page ID: " + verifyTuple.getIntFld(2));
            // System.out.println("Stored RID Slot: " + verifyTuple.getIntFld(3));
            //System.out.println("Vector being stored: " + Arrays.toString(verifyTuple.get100DVectorFld(1).getValues()));
        }
        verifyScan.closescan();

    }



    public int hash_function(double[] r, double[] v, int b, int w)
    {

        return (int)Math.floor((dot_product(r, v) + (double)b)/(double)w);
    }

    public double dot_product(double v1[], double v2[])
    {
        double product = 0;
        int n = 100;
        for(int i = 0; i < n; i++)
        {
            product = product + v1[i] * v2[i];
        }

        return product;
    }

    public LSHFFile(String fileName, int h, int L) throws Exception, ConstructPageException, IOException, GetFileEntryException, FileIOException, AddFileEntryException, PinPageException, InvalidPageNumberException, ReplacerException, ReplacerException, DiskMgrException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException
    {
        this.hashes = h;
        this.layers = L;
        //System.out.println("We are setting up header information");
        //System.out.flush();
        headerPage = new LSHFHeaderPage(new PageId());
        headerPage.setNumLayers(L);
        headerPage.setNumHashes(h);

        btreeIndex = new LSHFBTreeFile[L];

        for (int i = 0; i < L; i++) {
            String indexName = fileName+"BTree_layer" + i;
            PageId pageId = SystemDefs.JavabaseDB.get_file_entry(indexName);

            if (pageId != null) {
                // ✅ Reload existing index
                //System.out.println("✅ Loading existing B+Tree index: " + indexName + " (PageId: " + pageId.pid + ")");
                this.btreeIndex[i] = new LSHFBTreeFile(indexName);
            } else {
                // ❌ No existing index found, create a new one
                //System.out.println("⚠️ No existing BTree index found. Creating new one for " + indexName);
                this.btreeIndex[i] = new LSHFBTreeFile(indexName, AttrType.attrString, 250, 1);
            }
        }

        //initalizeHashFunction();
        try {
            //System.out.println(" Hash functions found in HeapFile. Retrieving");
            loadHashFunctions();
        } catch (Exception e) {
            //System.out.println(" Hash functions not found in HeapFile. Generating new ones...");
            initalizeHashFunction();
            saveHashFunctions();
        }

        // System.out.println("We have initalized the hash funciton");
        // System.out.flush();

    }

    public ArrayList<RID> rangeSearch(Vector100Dtype key, int distance) throws SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {
        ArrayList<Integer> queryHashes = new ArrayList<>();
        ArrayList<Integer> queryLayers = new ArrayList<>();
        ArrayList<RID> results = new ArrayList<>();

        for(int i = 0; i < layers; i++)
        {
            //System.out.println("Hashes for layer: " + i);
            for(int j = 0; j < hashes; j++)
            {
                int hash_value = hash_function(projections[i][j], convertShortToDouble(key.getValues()), b_values[i][j], width);
                queryHashes.add(hash_value);
                queryLayers.add(i);
                //System.out.println("We are in range search and hashes the value: " + hash_value);
            }
        }

        for(int i = 0; i < queryHashes.size(); i++)
        {
            int queryHash = queryHashes.get(i);
            int queryLayer = queryLayers.get(i);
            int bucketKey = (queryLayer * 100000) + queryHash; 

            // System.out.println("Current stored buckets in HashBucketTable:");
            // for (Integer keyVal : headerPage.getHashBucketTable().keySet()) {
            //     System.out.println("Stored Key: " + keyVal);
            // }

            //System.out.println("\nprinting bucket key value: " + bucketKey);
            if(!headerPage.getHashBucketTable().containsKey(bucketKey))
            {
                System.out.println("We printing this and dipping out of here");
                continue;
            }
                
    

            //System.out.println("Do we make it to this point in range search?");
            PageId pageId = headerPage.getHashBucketTable().get(bucketKey);
            //System.out.println("pageId + " + pageId);
            Heapfile bucketFile = new Heapfile("bucket_" + pageId.pid + "_" + queryLayer);
            Scan heapScan = bucketFile.openScan();
            Tuple t;
            RID curRid = new RID();
            
            //System.out.println("Checking all records in bucket: " + "bucket_" + pageId.pid + "_" + queryLayer);
            while((t = heapScan.getNext(new RID())) != null)
            {
                AttrType[] attrTypes = new AttrType[]{
                    new AttrType(AttrType.attrVector100D),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger)
                };

                short[] strSize = new short[0];
                t.setHdr((short) 3, attrTypes, strSize);
                Vector100Dtype stored_vector = t.get100DVectorFld(1);

                RID storedRid = new RID(new PageId(t.getIntFld(2)), t.getIntFld(3));

                double distance_to_query = stored_vector.computeDistance(key, stored_vector);

                // Debug: Check if the stored RIDs are unique
                //System.out.println("Stored RID: Page " + storedRid.pageNo.pid + ", Slot " + storedRid.slotNo);
                
                // Debug: Print tuple bytes to check raw storage
                //System.out.println("Tuple Raw Data: " + Arrays.toString(t.returnTupleByteArray()));
                

                //System.out.println("Printing the distance to query: " + distance_to_query);
                if(distance_to_query <= distance)
                {
                    if (!results.contains(storedRid)) { // Prevent duplicate RIDs
                        results.add(storedRid);
                        //System.out.println("Tuple Raw Data: " + Arrays.toString(stored_vector.getValues()));
                    }
                    //results.add(storedRid);
                }
            }

            heapScan.closescan();
        }
        return results;
    }

    public KeyDataEntry[] NN_Search(Vector100Dtype key, int k) throws ScanIteratorException, InsertException, LeafDeleteException, IteratorException, IndexSearchException, DeleteRecException, ConvertException, NodeNotMatchException, PinPageException, UnpinPageException, ConstructPageException, IndexInsertRecException, LeafInsertRecException, KeyNotMatchException, KeyTooLongException, SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {

        String[] bucketKeys = new String[layers]; 

        HashSet<String> uniqueVectors = new HashSet<>();

        for(int i = 0; i < layers; i++)
        {
            String bucketKey = "layer" + i;
            for(int j = 0; j < hashes; j++)
            {
                int hash_value = hash_function(projections[i][j], convertShortToDouble(key.getValues()), b_values[i][j], width);
                bucketKey += ("_" + hash_value);
            }
            bucketKeys[i] = bucketKey;
        }
        
        ArrayList<AbstractMap.SimpleEntry<KeyDataEntry, Double>> distance_to_query = new ArrayList<>();
        

        for(int i = 0; i < layers; i++)
        {
            ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
            //System.out.println("Printing bucket key: " + bucketKeys[i] + " within layer: " + i);
            if(k != 0)
                nearestNeighbors = btreeIndex[i].NNSearch(bucketKeys[i], k);
            else
            {
                LSHFFileScan scan = new LSHFFileScan(btreeIndex[i]);
                nearestNeighbors = scan.LSHFFileScan();
            }
            
            

            for(int j = 0; j < nearestNeighbors.size(); j++)
            {
                KeyDataEntry entry = nearestNeighbors.get(j);


                Vector100Dtype contender = ((Vector100DKey) entry.key).getKey();

                double distance = key.computeDistance(key, contender);

                //System.out.println("Printing vector: " + distance);

                String vectorKey = Arrays.toString(contender.getValues());

                
                if (!uniqueVectors.contains(vectorKey)) {
                    uniqueVectors.add(vectorKey);
                    distance_to_query.add(new AbstractMap.SimpleEntry<>(entry, distance));
                }
                
            }

        }

                // Sort
        Collections.sort(distance_to_query, Comparator.comparing(AbstractMap.SimpleEntry::getValue));
        
        if(k != 0)
        {
            ArrayList<AbstractMap.SimpleEntry<KeyDataEntry, Double>> topKNeighbors = new ArrayList<>(distance_to_query.subList(0, Math.min(k, distance_to_query.size())));
            KeyDataEntry[] returnValues  = new KeyDataEntry[k];
            int i = 0;
            for (AbstractMap.SimpleEntry<KeyDataEntry, Double> pair : topKNeighbors) {
                KeyDataEntry nearestEntry = pair.getKey();
                double distance = pair.getValue();
                
                RID rid = null;
                rid = ((LeafData) nearestEntry.data).getData();
                //rid.pageNo.pid + ", Slot: " + rid.slotNo
                //System.out.println("NN: " + nearestEntry.key +  " RID.pid: " + rid.pageNo.pid + " RID.slotNum: " + rid.slotNo + " | Distance: " + distance);
                returnValues[i] = nearestEntry;
                i++;

            }

            return returnValues; 
        }
        else
        {
            //ArrayList<AbstractMap.SimpleEntry<KeyDataEntry, Double>> topKNeighbors = new ArrayList<>(distance_to_query.subList(0, Math.min(k, distance_to_query.size())));
            KeyDataEntry[] returnValues  = new KeyDataEntry[distance_to_query.size()];
            int i = 0;
            for (AbstractMap.SimpleEntry<KeyDataEntry, Double> pair : distance_to_query) {
                KeyDataEntry nearestEntry = pair.getKey();
                double distance = pair.getValue();
                
                RID rid = null;
                rid = ((LeafData) nearestEntry.data).getData();
                //rid.pageNo.pid + ", Slot: " + rid.slotNo
                //System.out.println("NN: " + nearestEntry.key +  " RID.pid: " + rid.pageNo.pid + " RID.slotNum: " + rid.slotNo + " | Distance: " + distance);
                returnValues[i] = nearestEntry;
                i++;

            }

            return returnValues; 
        }


        
    }

    public KeyDataEntry[] Range_Search(Vector100Dtype key, double range) throws ScanIteratorException, InsertException, LeafDeleteException, IteratorException, IndexSearchException, DeleteRecException, ConvertException, NodeNotMatchException, PinPageException, UnpinPageException, ConstructPageException, IndexInsertRecException, LeafInsertRecException, KeyNotMatchException, KeyTooLongException, SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {

        String[] bucketKeys = new String[layers]; 

        HashSet<String> uniqueVectors = new HashSet<>();

        for(int i = 0; i < layers; i++)
        {
            String bucketKey = "layer" + i;
            for(int j = 0; j < hashes; j++)
            {
                int hash_value = hash_function(projections[i][j], convertShortToDouble(key.getValues()), b_values[i][j], width);
                bucketKey += ("_" + hash_value);
            }
            bucketKeys[i] = bucketKey;
        }
        
        ArrayList<AbstractMap.SimpleEntry<KeyDataEntry, Double>> distance_to_query = new ArrayList<>();

        for(int i = 0; i < layers; i++)
        {
            //System.out.println("Printing bucket key: " + bucketKeys[i] + " within layer: " + i);
            ArrayList<KeyDataEntry> nearestNeighbors = btreeIndex[i].RangeSearch(bucketKeys[i], key, range);
            
            

            for(int j = 0; j < nearestNeighbors.size(); j++)
            {
                KeyDataEntry entry = nearestNeighbors.get(j);


                Vector100Dtype contender = ((Vector100DKey) entry.key).getKey();

                double distance = key.computeDistance(key, contender);

                //System.out.println("Printing vector: " + distance);

                String vectorKey = Arrays.toString(contender.getValues());

                
                if (!uniqueVectors.contains(vectorKey) && distance < range) {
                    uniqueVectors.add(vectorKey);
                    distance_to_query.add(new AbstractMap.SimpleEntry<>(entry, distance));
                }
                
            }

        }

                // Sort
        Collections.sort(distance_to_query, Comparator.comparing(AbstractMap.SimpleEntry::getValue));
        //ArrayList<AbstractMap.SimpleEntry<KeyDataEntry, Double>> topKNeighbors = new ArrayList<>(distance_to_query.subList(0, Math.min(k, distance_to_query.size())));

        KeyDataEntry[] returnValues  = new KeyDataEntry[distance_to_query.size()];
        int i = 0;
        for (AbstractMap.SimpleEntry<KeyDataEntry, Double> pair : distance_to_query) {
            KeyDataEntry nearestEntry = pair.getKey();
            double distance = pair.getValue();
            
            RID rid = null;
            rid = ((LeafData) nearestEntry.data).getData();
            //rid.pageNo.pid + ", Slot: " + rid.slotNo
            //System.out.println("✅ NN: " + nearestEntry.key +  " RID.pid: " + rid.pageNo.pid + " RID.slotNum: " + rid.slotNo + " | Distance: " + distance);
            returnValues[i] = nearestEntry;
            i++;

        }

        return returnValues; 
    }


    public KeyDataEntry[] LSHFFileScan() throws ScanIteratorException, InsertException, LeafDeleteException, IteratorException, IndexSearchException, DeleteRecException, ConvertException, NodeNotMatchException, PinPageException, UnpinPageException, ConstructPageException, IndexInsertRecException, LeafInsertRecException, KeyNotMatchException, KeyTooLongException, SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {

        HashSet<String> uniqueVectors = new HashSet<>();


       
        ArrayList<KeyDataEntry> finalValues = new ArrayList<>();

        for(int i = 0; i < layers; i++)
        {
            //System.out.println("Printing bucket key: " + bucketKeys[i] + " within layer: " + i);
            LSHFFileScan scan = new LSHFFileScan(btreeIndex[i]);
            ArrayList<KeyDataEntry> nearestNeighbors = scan.LSHFFileScan();
            
           
            finalValues.addAll(nearestNeighbors);

            

            //System.out.println("size of nearestNeighbors: " + nearestNeighbors.size());

            // for(int j = 0; j < nearestNeighbors.size(); j++)
            // {
                


            //     //Vector100Dtype contender = ((Vector100DKey) entry.key).getKey();

            //     //String vectorKey = Arrays.toString(contender.getValues());

                
            //     // if (!uniqueVectors.contains(vectorKey)) {
            //     //     uniqueVectors.add(vectorKey);
            //     //     distance_to_query.add(new AbstractMap.SimpleEntry<>(entry, distance));
            //     // }
                
            // }

        }
        
        //Collections.sort(distance_to_query, Comparator.comparing(AbstractMap.SimpleEntry::getValue));

        KeyDataEntry[] returnValues  = new KeyDataEntry[finalValues.size()];

        for(int i = 0; i < finalValues.size(); i++)
        {
            returnValues[i] = finalValues.get(i);
        }

        

        return returnValues; 
    }

    @Override
    public void insert(final KeyClass data, final RID rid)
    {
        
        throw new UnsupportedOperationException("Delete operation is not yet implemented in LSHFFile.");
    }

    @Override
    public boolean Delete(final KeyClass data, final RID rid)  
    throws  DeleteFashionException, 
	    LeafRedistributeException,
	    RedistributeException,
	    InsertRecException,
	    KeyNotMatchException, 
	    UnpinPageException, 
	    IndexInsertRecException,
	    FreePageException, 
	    RecordNotFoundException, 
	    PinPageException,
	    IndexFullDeleteException, 
	    LeafDeleteException,
	    IteratorException, 
	    ConstructPageException, 
	    DeleteRecException,
	    IndexSearchException, 
	    IOException {
        
        throw new UnsupportedOperationException("Delete operation is not yet implemented in LSHFFile.");
    }

    public static void main(String[] args) {
        System.out.println("LSHFFile Compiled & Running!");
    }

}