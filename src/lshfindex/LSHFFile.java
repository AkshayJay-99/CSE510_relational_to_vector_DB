

package lshfindex;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
//import btree.*;
import java.util.Random;
import java.lang.Math;
import java.util.PriorityQueue;
import java.util.ArrayList;
//import RIDDistancePair;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

public class LSHFFile// extends IndexFile
{
    private int layers;
    private int hashes;

    private LSHFHeaderPage headerPage;
    private Heapfile heapFile;
    private BufMgr bufferManager;

    private int width = 200;
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

    public double[] convertShortToDouble(short[] inputArray)
    {
        double[] convertedArray = new double[inputArray.length];
        for(int i = 0; i < inputArray.length; i++)
            convertedArray[i] = (double) inputArray[i];
        return convertedArray;
    }


    public void insert(Vector100Dtype key, RID rid) throws  Exception, SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {
        double[] vector = convertShortToDouble(key.getValues());
        //System.out.println("We made it to the insert method");
        //System.out.flush();
        for(int i = 0; i < layers; i++)
        {
            //System.out.println("Starting on layer: " + i);
            int[] hash_list = new int[hashes];
            
            int layer = i;

            String bucketKey = "layer" + i;

            btreeIndex[layer] = new LSHFBTreeFile("BTree_layer" + layer, AttrType.attrString, 250, 1);
            PageId currentNodePage = btreeIndex[layer].getRootPageId();
            PageId childPage = null;

            for(int j = 0; j < hashes; j++)
            {
                //int layer = i;
                int hashVal = hash_function(projections[i][j], vector, b_values[i][j], width);
                hash_list[layer] = hashVal;
                bucketKey += ("_" + hashVal);
                //System.out.println("We are hashing into the " + hashVal + " In layer: " + i);
                //storeInHeapFile(hashVal, layer, key, rid);
                
                
                //btreeIndex[layer].insert( new StringKey(bucketKey), rid);

                if (j < hashes - 1) {
                    System.out.println("🛠 Storing Internal Key in Internal Node: " + bucketKey);
                    childPage = btreeIndex[layer].insertInternal(new StringKey(bucketKey), currentNodePage);
                    currentNodePage = childPage;
                 }
            }

            //System.out.println("new bucket key: " + bucketKey);

            Vector100DKey vectorKey = new Vector100DKey(key);
            System.out.println("📌 Inserting Leaf Node: " + vectorKey + " | RID -> Page: " + rid.pageNo.pid + ", Slot: " + rid.slotNo);
            btreeIndex[layer].insertLeaf(vectorKey, rid, bucketKey);
            
        }

        printBTreeStructure(0);
       
    }



    public void printBTreeStructure(int layer) {
        if (btreeIndex[layer] == null) {
            System.out.println("⚠️ BTree for layer " + layer + " is not initialized.");
            return;
        }

        try {
            System.out.println("\n🔍 **BTree Structure for Layer " + layer + "**");
            LSHFBTFileScan scan = btreeIndex[layer].new_scan(null, null);
            KeyDataEntry entry;

            while ((entry = scan.get_next()) != null) {
                if (entry.key instanceof StringKey) {
                    String key = ((StringKey) entry.key).getKey();

                    if (entry.data instanceof LeafData) {
                        RID rid = ((LeafData) entry.data).getData();
                        System.out.println("📌 Leaf Node | Key: " + key + " | RID -> Page: " + rid.pageNo.pid + ", Slot: " + rid.slotNo);
                    } 
                    else if (entry.data instanceof IndexData) {
                        PageId pageId = ((IndexData) entry.data).getData();
                        System.out.println("🗂️ Internal Node | Key: " + key + " | Points to Page: " + pageId.pid);
                    } 
                    else {
                        System.out.println("⚠️ Unknown Node Type for Key: " + key + " | Data: " + entry.data.getClass().getSimpleName());
                    }
                }
            }

            scan.DestroyLSHFBTreeFileScan();
            System.out.println("✅ End of BTree structure for layer " + layer + "\n");
        } catch (Exception e) {
            System.out.println("❌ Error printing BTree structure for layer " + layer + ": " + e.getMessage());
        }
    }






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

    public LSHFFile(String fileName, int h, int L) throws ConstructPageException
    {
        this.hashes = h;
        this.layers = L;
        //System.out.println("We are setting up header information");
        //System.out.flush();
        headerPage = new LSHFHeaderPage(new PageId());
        headerPage.setNumLayers(L);
        headerPage.setNumHashes(h);

         btreeIndex = new LSHFBTreeFile[L];

        initalizeHashFunction();
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

    public ArrayList<RID> NN_Search(Vector100Dtype key, int k) throws SpaceNotAvailableException, InvalidSlotNumberException, HFDiskMgrException, DiskMgrException, BufMgrException, PageNotReadException, PageUnpinnedException, PagePinnedException, InvalidFrameNumberException, HashEntryNotFoundException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, BufferPoolExceededException, HFException, HashOperationException, ReplacerException, HFBufMgrException
    {
        ArrayList<Integer> queryHashes = new ArrayList<>();
        ArrayList<Integer> queryLayers = new ArrayList<>();
        ArrayList<RIDDistancePair> results = new ArrayList<>();
        PriorityQueue<RIDDistancePair> maxHeap = new PriorityQueue<>((a,b) -> Double.compare(b.distance, a.distance));
        ArrayList<RID> nearestNeightbors = new ArrayList<>();

        for(int i = 0; i < layers; i++)
        {
            for(int j = 0; j < hashes; j++)
            {
                int hash_value = hash_function(projections[i][j], convertShortToDouble(key.getValues()), b_values[i][j], width);
                queryHashes.add(hash_value);
                queryLayers.add(i);
            }
        }

        for(int i = 0; i < queryHashes.size(); i++)
        {
            int queryHash = queryHashes.get(i);
            int queryLayer = queryLayers.get(i);
            int bucketKey = (queryLayer * 100000) + queryHash; 
            if(!headerPage.getHashBucketTable().containsKey(bucketKey))
                continue;
            
            PageId pageId = headerPage.getHashBucketTable().get(bucketKey);
            // System.out.flush();
            // System.out.println("Scanning Bucket File: bucket_" + pageId.pid);
            // System.out.flush();
            Heapfile bucketFile = new Heapfile("bucket_" + pageId.pid + "_" + queryLayer);
            Scan heapScan = bucketFile.openScan();
           
            Tuple t;
            RID curRid = new RID();
            

            // // Print tuple contents
            // System.out.println("Retrieved Tuple Schema:");
            // System.out.println("Vector Field (1): " + t.get100DVectorFld(1));  // Check if vector field is valid
            // System.out.println("RID Page ID (2): " + t.getIntFld(2));
            // System.out.println("RID Slot Number (3): " + t.getIntFld(3));

            while(true)
            {
                //RID curRid = new RID();
                t = heapScan.getNext(curRid);
                if(t == null)
                {
                    //System.out.println("ERROR: heapScan.getNext() returned NULL. Ending scan.");
                    break;
                }

                if (curRid == null || curRid.pageNo.pid < 0 || curRid.slotNo < 0) {  
                    System.out.println("ERROR: Retrieved invalid RID -> Page: " + (curRid == null ? "NULL" : curRid.pageNo.pid) + ", Slot: " + (curRid == null ? "NULL" : curRid.slotNo) + ". Skipping...");
                    continue;
                }

                AttrType[] attrTypes = new AttrType[]{
                    new AttrType(AttrType.attrVector100D),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger)
                };

                short[] strSize = new short[0];
                t.setHdr((short) 3, attrTypes, strSize);

                // System.out.println("Retrieved Tuple -> Page: " + curRid.pageNo.pid + ", Slot: " + curRid.slotNo);
                // System.out.println("Stored Vector: " + t.get100DVectorFld(1)); 
                // System.out.println("Stored RID Page ID: " + t.getIntFld(2));
                // System.out.println("Stored RID Slot: " + t.getIntFld(3));

                Vector100Dtype stored_vector = t.get100DVectorFld(1);
                //System.out.println(stored_vector);

                RID storedRid = new RID(new PageId(t.getIntFld(2)), t.getIntFld(3));
                //System.out.println("Fetching RID: Page " + storedRid.pageNo.pid + ", Slot " + storedRid.slotNo);
                if (storedRid.slotNo < 0 || storedRid.pageNo.pid < 0) {
                    System.out.println("ERROR: Invalid slot number detected.");
                    continue;  // Skip invalid RIDs
                }

                double distance_to_query = stored_vector.computeDistance(key, stored_vector);
                if (maxHeap.size() < k) {
                    maxHeap.add(new RIDDistancePair(storedRid, distance_to_query));
                } else if (distance_to_query < maxHeap.peek().distance) {
                    maxHeap.poll(); 
                    maxHeap.add(new RIDDistancePair(storedRid, distance_to_query));
                }
                
            }
            heapScan.closescan();

        }

        while (!maxHeap.isEmpty())
        {
            nearestNeightbors.add(maxHeap.poll().rid);
        }
        return nearestNeightbors; 
    }

    public static void main(String[] args) {
        System.out.println("LSHFFile Compiled & Running!");
    }

}