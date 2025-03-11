/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */

package lshfindex;
import java.io.*;
import java.util.Arrays;
import java.lang.*;
import global.*;
import diskmgr.*;
import heap.*;

/**
 * A LSHFBTLeafPage is a leaf page on a B+ tree.  It holds abstract 
 * <key, RID> pairs; it doesn't know anything about the keys 
 * (their lengths or their types), instead relying on the abstract
 * interface consisting of BT.java.
 */
public class LSHFBTLeafPage extends LSHFBTSortedPage {
  
  /** pin the page with pageno, and get the corresponding LSHFBTLeafPage,
   * also it sets the type to be NodeType.LEAF.
   *@param pageno Input parameter. To specify which page number the
   *  LSHFBTLeafPage will correspond to.
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *    Input parameter.   
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException LSHFBTLeafPage constructor error
   */
  public LSHFBTLeafPage(PageId pageno, int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(pageno, keyType);
      setType(NodeType.LEAF);
    }
  
  /**associate the LSHFBTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF. 
   *@param page  input parameter. To specify which page  the
   *  LSHFBTLeafPage will correspond to.
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *  Input parameter.    
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException LSHFBTLeafPage constructor error
   */
  public LSHFBTLeafPage(Page page, int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(page, keyType);
      setType(NodeType.LEAF);
    }  
  
  /**new a page, associate the LSHFBTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF. 
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *  Input parameter.
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException LSHFBTLeafPage constructor error
   */
  public LSHFBTLeafPage( int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(keyType);
      setType(NodeType.LEAF);
    }  
  

  
  /** insertRecord
   * READ THIS DESCRIPTION CAREFULLY. THERE ARE TWO RIDs
   * WHICH MEAN TWO DIFFERENT THINGS.
   * Inserts a key, rid value into the leaf node. This is
   * accomplished by a call to SortedPage::insertRecord()
   *  Parameters:
   *@param key - the key value of the data record. Input parameter.
   *@param dataRid - the rid of the data record. This is
   *               stored on the leaf page along with the
   *               corresponding key value. Input parameter.
   *
   *@return - the rid of the inserted leaf record data entry,
   *           i.e., the <key, dataRid> pair.
   *@exception  LeafInsertRecException error when insert
   */   
  public RID insertRecord(KeyClass key, RID dataRid) 
    throws  LeafInsertRecException
    {
      KeyDataEntry entry;
      
      try {
        
        if(key instanceof StringKey) 
        {
          entry = new KeyDataEntry((StringKey) key, dataRid);
        }
        else if(key instanceof Vector100DKey)
        {
          System.out.println("We setting then entry value");
          entry = new KeyDataEntry((Vector100DKey) key, dataRid);
        }
        else
        {
          throw new LeafInsertRecException(null, "Unsupported Type");
        }
        System.out.println("Checking in leaf page: " + entry.key);
        return insertRecord(entry);
      }
      catch(Exception e) {
        throw new LeafInsertRecException(e, "insert record failed");
      }
    } // end of insertRecord
  
  
  /**  Iterators. 
   * One of the two functions: getFirst and getNext
   * which  provide an iterator interface to the records on a LSHFBTLeafPage.
   *@param rid It will be modified and the first rid in the leaf page
   * will be passed out by itself. Input and Output parameter.
   *@return return the first KeyDataEntry in the leaf page.
   * null if no more record
   *@exception  IteratorException iterator error
   */
  public KeyDataEntry getFirst(RID rid) 
    throws  IteratorException
    {
      
      KeyDataEntry  entry; 
      
      try {
        rid.pageNo = getCurPage();
        rid.slotNo = 0; // begin with first slot
	
        if ( getSlotCnt() <= 0) {
          return null;
        }

        entry=LSHFBT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0), keyType, NodeType.LEAF);
	
        return entry;
      }
      catch (Exception e) {
	throw new IteratorException(e, "Get first entry failed");
      }
    } // end of getFirst

 
   /**Iterators.  
    * One of the two functions: getFirst and getNext which  provide an
    * iterator interface to the records on a LSHFBTLeafPage.
    *@param rid It will be modified and the next rid will be passed out 
    *by itself. Input and Output parameter.
    *@return return the next KeyDataEntry in the leaf page. 
    *null if no more record.
    *@exception IteratorException iterator error
    */

   public KeyDataEntry getNext (RID rid)
     throws  IteratorException
   {
     KeyDataEntry  entry; 
     int i;
     try{
       rid.slotNo++; //must before any return;
       i=rid.slotNo;
       
       if ( rid.slotNo >= getSlotCnt())
       {
	 return null;
       }
       
       entry=LSHFBT.getEntryFromBytes(getpage(),getSlotOffset(i), getSlotLength(i),
                  keyType, NodeType.LEAF);
       
       return entry;
     } 
     catch (Exception e) {
       throw new IteratorException(e,"Get next entry failed");
     }
  }
  
  
  
  /**
   * getCurrent returns the current record in the iteration; it is like
   * getNext except it does not advance the iterator.
   *@param rid  the current rid. Input and Output parameter. But
   *    Output=Input.
   *@return return the current KeyDataEntry
   *@exception  IteratorException iterator error
   */ 
   public KeyDataEntry getCurrent (RID rid)
       throws  IteratorException
   {  
     rid.slotNo--;
     return getNext(rid);
   }
  
  
  /** 
   * delete a data entry in the leaf page.
   *@param dEntry the entry will be deleted in the leaf page. Input parameter.
   *@return true if deleted; false if no dEntry in the page
   *@exception LeafDeleteException error when delete
   */
   public boolean delEntry (KeyDataEntry dEntry)
     throws  LeafDeleteException
    {
      KeyDataEntry  entry;
      RID rid=new RID(); 
      
      try {
	for(entry = getFirst(rid); entry!=null; entry=getNext(rid)) 
	  {  
	    if ( entry.equals(dEntry) ) {
	      if ( super.deleteSortedRecord( rid ) == false )
		throw new LeafDeleteException(null, "Delete record failed");
	      return true;
	    }
	    
	 }
	return false;
      } 
      catch (Exception e) {
	throw new LeafDeleteException(e, "delete entry failed");
      }
      
    } // end of delEntry

  /*used in full delete 
   *@param leafPage the sibling page of this. Input parameter.
   *@param parentIndexPage the parant of leafPage and this. Input parameter.
   *@param direction -1 if "this" is left sibling of leafPage ; 
   *      1 if "this" is right sibling of leafPage. Input parameter.
   *@param deletedKey the key which was already deleted, and cause 
   *        redistribution. Input parameter.
   *@exception LeafRedistributeException
   *@return true if redistrbution success. false if we can not redistribute them.
   */
  boolean redistribute(LSHFBTLeafPage leafPage, LSHFBTIndexPage parentIndexPage, 
		       int direction, KeyClass deletedKey)
    throws LeafRedistributeException
    {
      boolean st;
      // assertion: leafPage pinned
      try {
	if (direction ==-1) { // 'this' is the left sibling of leafPage
	  if ( (getSlotLength(getSlotCnt()-1) + available_space()+ 8 /*  2*sizeof(slot) */) > 
	       ((MAX_SPACE-DPFIXED)/2)) {
            // cannot spare a record for its underflow sibling
            return false;
	  }
	  else {
            // move the last record to its sibling
	    
            // get the last record 
            KeyDataEntry lastEntry;
            lastEntry=LSHFBT.getEntryFromBytes(getpage(),getSlotOffset(getSlotCnt()-1)
					   ,getSlotLength(getSlotCnt()-1), keyType, NodeType.LEAF);
	    
	    
            //get its sibling's first record's key for adjusting parent pointer
            RID dummyRid=new RID();
            KeyDataEntry firstEntry;
            firstEntry=leafPage.getFirst(dummyRid);

            // insert it into its sibling            
            leafPage.insertRecord(lastEntry);
            
            // delete the last record from the old page
            RID delRid=new RID();
            delRid.pageNo = getCurPage();
            delRid.slotNo = getSlotCnt()-1;
            if ( deleteSortedRecord(delRid) == false )
	      throw new LeafRedistributeException(null, "delete record failed");

	    
            // adjust the entry pointing to sibling in its parent
            if (deletedKey != null)
                st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
            else 
                st = parentIndexPage.adjustKey(lastEntry.key,
                                            firstEntry.key);
            if (st == false) 
	      throw new LeafRedistributeException(null, "adjust key failed");
            return true;
	  }
	}
	else { // 'this' is the right sibling of pptr
	  if ( (getSlotLength(0) + available_space()+ 8) > ((MAX_SPACE-DPFIXED)/2)) {
            // cannot spare a record for its underflow sibling
            return false;
	  }
	  else {
            // move the first record to its sibling
	    
            // get the first record
            KeyDataEntry firstEntry;
            firstEntry=LSHFBT.getEntryFromBytes(getpage(), getSlotOffset(0),
					    getSlotLength(0), keyType,
					    NodeType.LEAF);
	    
            // insert it into its sibling
            RID dummyRid=new RID();
            leafPage.insertRecord(firstEntry);
            

            // delete the first record from the old page
            RID delRid=new RID();
            delRid.pageNo = getCurPage();
            delRid.slotNo = 0;
            if ( deleteSortedRecord(delRid) == false) 
	      throw new LeafRedistributeException(null, "delete record failed");  
	    
	    
            // get the current first record of the old page
            // for adjusting parent pointer.
            KeyDataEntry tmpEntry;
            tmpEntry = getFirst(dummyRid);
         
            
            // adjust the entry pointing to itself in its parent
            st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
            if( st==false) 
	      throw new LeafRedistributeException(null, "adjust key failed"); 
            return true;
	  }
	}
      }
      catch (Exception e) {
	throw new LeafRedistributeException(e, "redistribute failed");
      } 
    } // end of redistribute


    public void clearPage() {
    System.out.println("🆑 Clearing all records from leaf page before inserting new data.");
    try {
        RID rid = new RID();
        KeyDataEntry entry = getFirst(rid);

        while (entry != null) {
            if (!delEntry(entry)) {
                System.out.println("❌ ERROR: Failed to delete entry: " + entry.key);
            }
            entry = getFirst(rid); // Get next entry after deletion
        }
    } catch (Exception e) {
        System.out.println("❌ ERROR: Failed to clear leaf page: " + e.getMessage());
    }
}

public void printLeafPageContents() {
    System.out.println("🔍 DEBUG: Dumping Raw Leaf Page Contents...");

    try {
        System.out.println("📌 Leaf Page ID: " + getCurPage().pid);
        System.out.println("📌 Number of Records: " + numberOfRecords());
        System.out.println("📌 Free Space: " + available_space());

        // ✅ Print the slot directory
        System.out.println("🔍 DEBUG: Printing Slot Directory...");
        for (int i = 0; i < numberOfRecords(); i++) {
            System.out.println("📌 Slot " + i + " Offset: " + getSlotOffset(i) + " Length: " + getSlotLength(i));
        }

        // ✅ Print each record manually
        System.out.println("🔍 DEBUG: Reading Entries Manually...");
        RID rid = new RID();
        KeyDataEntry entry = getFirst(rid);

        if (entry == null) {
            System.out.println("⚠️ WARNING: No valid records found in leaf page!");
        }

        while (entry != null) {
            System.out.println("📌 Stored Key: " + entry.key);
            entry = getNext(rid);
        }

    } catch (Exception e) {
        System.out.println("❌ ERROR: Failed to retrieve leaf page contents: " + e.getMessage());
    }

    // 🔍 Dump Raw Byte Contents
    try {
        byte[] rawData = new byte[10000];  // Assuming a 10,000-byte page
        System.arraycopy(data, 0, rawData, 0, 10000);
        System.out.println("🔍 RAW PAGE DUMP (First 256 bytes): " + Arrays.toString(Arrays.copyOfRange(rawData, 0, 256)));
    } catch (Exception e) {
        System.out.println("❌ ERROR: Could not dump raw leaf page memory: " + e.getMessage());
    }
}
  
} // end of LSHFBTLeafPage

    
 





















