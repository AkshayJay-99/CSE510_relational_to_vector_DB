/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package lshfindex;
import java.io.*;
import global.*;
import heap.*;

/**
 * BTFileScan implements a search/iterate interface to B+ tree 
 * index files (class BTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class LSHFFileScan extends IndexFileScan
             implements  GlobalConst
{
  private Heapfile heapFile;
  private Scan heapScan; 
  private Tuple tuple;
  private RID curRid;       // position in current leaf; note: this is 

  public LSHFFileScan(Heapfile file) throws InvalidTupleSizeException, IOException
  {
      heapFile = file;
      heapScan = heapFile.openScan();
      tuple = new Tuple();
      curRid = new RID();
  }

  public KeyDataEntry get_next() throws ScanIteratorException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException
  {
    if(heapScan == null) return null;

    Tuple t;
    while((t = heapScan.getNext(curRid)) != null)
    {
      int hashValue = t.getIntFld(1);
      RID stored_RID = new RID(new PageId(t.getIntFld(2)), t.getIntFld(3));

      return new KeyDataEntry(new IntegerKey(hashValue), new LeafData(stored_RID));
    }
    return null;
  }

  public int keysize() {

      return 8; // Example: if your key size is 8 bytes.
  }

  public void delete_current() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception
  {
    heapFile.deleteRecord(curRid);
  }

  public void close()
  {
    if(heapScan != null)
    {
      heapScan.closescan();
      heapScan = null;
    }
  }



}





