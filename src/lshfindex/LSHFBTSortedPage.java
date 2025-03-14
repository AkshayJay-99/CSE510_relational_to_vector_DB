/*
 * @(#) SortedPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *
 *      by Xiaohu Li (xiaohu@cs.wisc.edu)
 */

package lshfindex;

import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import heap.*;

import java.util.Arrays;


/**
 * LSHFBTSortedPage class 
 * just holds abstract records in sorted order, based 
 * on how they compare using the key interface from BT.java.
 */
public class LSHFBTSortedPage  extends HFPage{

  
  int keyType; //it will be initialized in BTFile
  
  
  /** pin the page with pageno, and get the corresponding SortedPage
   *@param pageno input parameter. To specify which page number the
   *  LSHFBTSortedPage will correspond to.
   *@param keyType input parameter. It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger. 
   *@exception  ConstructPageException  error for LSHFBTSortedPage constructor
   */
  public LSHFBTSortedPage(PageId pageno, int keyType) 
    throws ConstructPageException 
    { 
      super();
      try {
	// super();
	SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/); 
	this.keyType=keyType;   
      }
      catch (Exception e) {
	throw new ConstructPageException(e, "construct sorted page failed");
      }
    }
  
  /**associate the SortedPage instance with the Page instance 
   *@param page input parameter. To specify which page  the
   *  LSHFBTSortedPage will correspond to.
   *@param keyType input parameter. It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger. 
   */
  public LSHFBTSortedPage(Page page, int keyType) {
    
    super(page);
    this.keyType=keyType;   
  }  
  
  
  /**new a page, and associate the SortedPage instance with the Page instance
   *@param keyType input parameter. It specifies the type of key. It can be 
   *               AttrType.attrString or AttrType.attrInteger. 
   *@exception  ConstructPageException error for LSHFBTSortedPage constructor
   */ 
  public LSHFBTSortedPage(int keyType) 
    throws ConstructPageException
    {
      super();
      try{
	Page apage=new Page();
	PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
	if (pageId==null) 
	  throw new ConstructPageException(null, "construct new page failed");
	this.init(pageId, apage);
	this.keyType=keyType;   
      }
      catch (Exception e) {
        e.printStackTrace();
	throw new ConstructPageException(e, "construct sorted page failed");
      }
    }  
  
  /**
   * Performs a sorted insertion of a record on an record page. The records are
   *  sorted in increasing key order.
   *  Only the  slot  directory is  rearranged.  The  data records remain in
   *  the same positions on the  page.
   * 
   *@param entry the entry to be inserted. Input parameter.
   *@return its rid where the entry was inserted; null if no space left.
   *@exception  InsertRecException error when insert
   */
   protected RID insertRecord( KeyDataEntry entry)
          throws InsertRecException 
   {
     int i;
     short  nType;
     RID rid;
     byte[] record;
     // ASSERTIONS:
     // - the slot directory is compressed; Inserts will occur at the end
     // - slotCnt gives the number of slots used
     
     // general plan:
     //    1. Insert the record into the page,
     //       which is then not necessarily any more sorted
     //    2. Sort the page by rearranging the slots (insertion sort)
     
     try {
       
       //System.out.println("Checking call before fail: " + entry.key);
       record=LSHFBT.getBytesFromEntry(entry); 

        int recordSize = record.length;
        

        // 🔍 Debugging Output
        //System.out.println("🔍 DEBUG: Inserting record - Size: " + recordSize );


       rid=super.insertRecord(record);
       //System.out.println("successfully made it past insertRecord this is the RID in SortedPage: " + rid);
         if (rid==null) return null;
	 
         if ( entry.data instanceof LeafData )
          nType= NodeType.LEAF;
         else  //  entry.data instanceof IndexData              
	        nType= NodeType.INDEX;
	 
	//  System.out.println("🔍 DEBUG: Checking node type before insert: " + nType + 
  //                  " | Entry Type: " + entry.key.getClass().getSimpleName());
	 // performs a simple insertion sort
	 for (i=getSlotCnt()-1; i > 0; i--) 
	   {
	     
	     KeyClass key_i, key_iplus1;

	      // System.out.println("🔍 DEBUG: Extracting key at offset " + getSlotOffset(i) + 
        //            ", length " + getSlotLength(i));
        // byte[] rawData = new byte[getSlotLength(i)];
        // System.arraycopy(getpage(), getSlotOffset(i), rawData, 0, getSlotLength(i));

        // System.out.println("🔍 DEBUG: Raw Key Bytes: " + Arrays.toString(rawData));

        KeyClass key_iTester = LSHFBT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType, nType).key;
        

	     key_i=LSHFBT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType, nType).key;
	     
	     key_iplus1=LSHFBT.getEntryFromBytes(getpage(), getSlotOffset(i-1), getSlotLength(i-1), keyType, nType).key;

        // System.out.println("Extracted key_i: " + key_i);
        // System.out.println("Extracted key_iplus1: " + key_iplus1);

        
	     
        if (LSHFBT.keyCompare(key_i, key_iplus1) < 0)
        {
                // switch slots:
            int ln, off;
            ln= getSlotLength(i);
            off=getSlotOffset(i);
            setSlot(i,getSlotLength(i-1),getSlotOffset(i-1));  
            setSlot(i-1, ln, off);
        } else {
            // end insertion sort
            break;
	       }
	     
	   }
	 
	 // ASSERTIONS:
	 // - record keys increase with increasing slot number 
	 // (starting at slot 0)
	 // - slot directory compacted
	 
	 rid.slotNo = i;
	 return rid;
     }
     catch (Exception e ) { 
       throw new InsertRecException(e, "insert record failed"); 
     }
     
     
   } // end of insertRecord


  /**  Deletes a record from a sorted record page. It also calls
   *    HFPage.compact_slot_dir() to compact the slot directory.
   *@param rid it specifies where a record will be deleted
   *@return true if success; false if rid is invalid(no record in the rid).
   *@exception DeleteRecException error when delete
   */
  public  boolean deleteSortedRecord(RID rid)
    throws DeleteRecException
    {
      try {
	
	deleteRecord(rid);
	compact_slot_dir();
	return true;  
	// ASSERTIONS:
	// - slot directory is compacted
      }
      catch (Exception  e) {
	if (e instanceof InvalidSlotNumberException)
	  return false;
	else
	  throw new DeleteRecException(e, "delete record failed");
      }
    } // end of deleteSortedRecord
  
  /** How many records are in the page
   *@param return the number of records.
   *@exception IOException I/O errors
   */
  protected int numberOfRecords() 
    throws IOException
    {
      return getSlotCnt();
    }
};


