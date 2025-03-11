/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package lshfindex;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

/** btfile.java
 * This is the main definition of class BTreeFile, which derives from 
 * abstract base class IndexFile.
 * It provides an insert/delete interface.
 */
public class LSHFBTreeFile extends IndexFile 
  implements GlobalConst {
  
  private final static int MAGIC0=1989;
  
  private final static String lineSep=System.getProperty("line.separator");
  
  private static FileOutputStream fos;
  private static DataOutputStream trace;
  
  
  /** It causes a structured trace to be written to a
   * file.  This output is
   * used to drive a visualization tool that shows the inner workings of the
   * b-tree during its operations. 
   *@param filename input parameter. The trace file name
   *@exception IOException error from the lower layer
   */ 
  public static void traceFilename(String filename) 
    throws  IOException
    {
      
      fos=new FileOutputStream(filename);
      trace=new DataOutputStream(fos);
    }
  
  /** Stop tracing. And close trace file. 
   *@exception IOException error from the lower layer
   */
  public static void destroyTrace() 
    throws  IOException
    {
      if( trace != null) trace.close();
      if( fos != null ) fos.close();
      fos=null;
      trace=null;
    }
  
  
  private LSHFHeaderPage headerPage;
  private  PageId  headerPageId;
  private String  dbname;  
  
  /**
   * Access method to data member.
   * @return  Return a LSHFHeaderPage object that is the header page
   *          of this btree file.
   */
  public LSHFHeaderPage getHeaderPage() {
    return headerPage;
  }
  
  private PageId get_file_entry(String filename)         
    throws GetFileEntryException
    {
      try {
	return SystemDefs.JavabaseDB.get_file_entry(filename);
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new GetFileEntryException(e,"");
      }
    }
  
  
  
  private Page pinPage(PageId pageno) 
    throws PinPageException
    {
      try {
        Page page=new Page();
        SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
        return page;
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new PinPageException(e,"");
      }
    }
  
  private void add_file_entry(String fileName, PageId pageno) 
    throws AddFileEntryException
    {
      try {
        SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new AddFileEntryException(e,"");
      }      
    }
  
  private void unpinPage(PageId pageno) 
    throws UnpinPageException
    { 
      try{
        SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new UnpinPageException(e,"");
      } 
    }
  
  private void freePage(PageId pageno) 
    throws FreePageException
    {
      try{
	SystemDefs.JavabaseBM.freePage(pageno);    
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new FreePageException(e,"");
      } 
      
    }
  private void delete_file_entry(String filename)
    throws DeleteFileEntryException
    {
      try {
        SystemDefs.JavabaseDB.delete_file_entry( filename );
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new DeleteFileEntryException(e,"");
      } 
    }
  
  private void unpinPage(PageId pageno, boolean dirty) 
    throws UnpinPageException
    {
      try{
        SystemDefs.JavabaseBM.unpinPage(pageno, dirty);  
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new UnpinPageException(e,"");
      }  
    }
  
  
  
  
  /**  BTreeFile class
   * an index file with given filename should already exist; this opens it.
   *@param filename the B+ tree file name. Input parameter.
   *@exception GetFileEntryException  can not ger the file from DB 
   *@exception PinPageException  failed when pin a page
   *@exception ConstructPageException   BT page constructor failed
   */
  public LSHFBTreeFile(String filename)
    throws GetFileEntryException,  
	   PinPageException, 
	   ConstructPageException        
    {      
      
      
      headerPageId=get_file_entry(filename);   
      
      headerPage= new  LSHFHeaderPage( headerPageId);       
      dbname = new String(filename);
      /*
       *
       * - headerPageId is the PageId of this BTreeFile's header page;
       * - headerPage, headerPageId valid and pinned
       * - dbname contains a copy of the name of the database
       */
    }    
  
  
  /**
   *  if index file exists, open it; else create it.
   *@param filename file name. Input parameter.
   *@param keytype the type of key. Input parameter.
   *@param keysize the maximum size of a key. Input parameter.
   *@param delete_fashion full delete or naive delete. Input parameter.
   *           It is either DeleteFashion.NAIVE_DELETE or 
   *           DeleteFashion.FULL_DELETE.
   *@exception GetFileEntryException  can not get file
   *@exception ConstructPageException page constructor failed
   *@exception IOException error from lower layer
   *@exception AddFileEntryException can not add file into DB
   */
  public LSHFBTreeFile(String filename, int keytype,
		   int keysize, int delete_fashion)  
    throws GetFileEntryException, 
	   ConstructPageException,
	   IOException, 
	   AddFileEntryException
    {
      
      
      headerPageId=get_file_entry(filename);
      if( headerPageId==null) //file not exist
	{
	  headerPage= new  LSHFHeaderPage(); 
	  headerPageId= headerPage.getPageId();
	  add_file_entry(filename, headerPageId);
	  headerPage.set_magic0(MAGIC0);
	  headerPage.set_rootId(new PageId(INVALID_PAGE));
	  headerPage.set_keyType((short)keytype);    
	  headerPage.set_maxKeySize(keysize);
	  headerPage.set_deleteFashion( delete_fashion );
	  headerPage.setType(NodeType.BTHEAD);
	}
      else {
	headerPage = new LSHFHeaderPage( headerPageId );  
      }
      
      dbname=new String(filename);
      
    }
  
  /** Close the B+ tree file.  Unpin header page.
   *@exception PageUnpinnedException  error from the lower layer
   *@exception InvalidFrameNumberException  error from the lower layer
   *@exception HashEntryNotFoundException  error from the lower layer
   *@exception ReplacerException  error from the lower layer
   */
  public void close()
    throws PageUnpinnedException, 
	   InvalidFrameNumberException, 
	   HashEntryNotFoundException,
           ReplacerException
    {
      if ( headerPage!=null) {
	SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
	headerPage=null;
      }  
    }
  
  /** Destroy entire B+ tree file.
   *@exception IOException  error from the lower layer
   *@exception IteratorException iterator error
   *@exception UnpinPageException error  when unpin a page
   *@exception FreePageException error when free a page
   *@exception DeleteFileEntryException failed when delete a file from DM
   *@exception ConstructPageException error in BT page constructor 
   *@exception PinPageException failed when pin a page
   */
  public void destroyFile() 
    throws IOException, 
	   IteratorException, 
	   UnpinPageException,
	   FreePageException,   
	   DeleteFileEntryException, 
	   ConstructPageException,
	   PinPageException     
    {
      if( headerPage != null) {
	PageId pgId= headerPage.get_rootId();
	if( pgId.pid != INVALID_PAGE) 
	  _destroyFile(pgId);
	unpinPage(headerPageId);
	freePage(headerPageId);      
	delete_file_entry(dbname);
	headerPage=null;
      }
    }  
  
  
  private void  _destroyFile(PageId pageno) 
    throws IOException, 
	   IteratorException, 
	   PinPageException,
           ConstructPageException, 
	   UnpinPageException, 
	   FreePageException
    {
      
      LSHFBTSortedPage sortedPage;
      Page page=pinPage(pageno) ;
      sortedPage= new LSHFBTSortedPage( page, headerPage.get_keyType());
      
      if (sortedPage.getType() == NodeType.INDEX) {
	LSHFBTIndexPage indexPage= new LSHFBTIndexPage( page, headerPage.get_keyType());
	RID      rid=new RID();
	PageId       childId;
	KeyDataEntry entry;
	for (entry = indexPage.getFirst(rid);
	     entry!=null; entry = indexPage.getNext(rid))
	  { 
	    childId = ((IndexData)(entry.data)).getData();
	    _destroyFile(childId);
	  }
      } else { // LSHFBTLeafPage 
	
	unpinPage(pageno);
	freePage(pageno);
      }
      
    }
  
  private void  updateHeader(PageId newRoot)
    throws   IOException, 
	     PinPageException,
	     UnpinPageException
    {
      
      LSHFHeaderPage header;
      PageId old_data;
      
      
      header= new LSHFHeaderPage( pinPage(headerPageId));
      
      old_data = headerPage.get_rootId();
      header.set_rootId( newRoot);
      
      // clock in dirty bit to bm so our dtor needn't have to worry about it
      unpinPage(headerPageId, true /* = DIRTY */ );
      
      
      // ASSERTIONS:
      // - headerPage, headerPageId valid, pinned and marked as dirty
      
    }


	public void insertLeaf(Vector100DKey vectorKey, RID rid, String bucketKey) 
		throws IOException, 
			KeyTooLongException, 
			KeyNotMatchException, 
			LeafInsertRecException, 
			IndexInsertRecException,
			ConstructPageException, 
			UnpinPageException,
			PinPageException, 
			NodeNotMatchException, 
			ConvertException,
			DeleteRecException,
			IndexSearchException,
			IteratorException, 
			LeafDeleteException, 
			InsertException 
	{
		System.out.println("🍃 Searching for correct leaf node for: " + vectorKey);

		// ✅ Step 1: Start from the root
		PageId currentPageId = headerPage.get_rootId();
		LSHFBTLeafPage leafPage = null;

		if (currentPageId.pid == INVALID_PAGE) {
			System.out.println("⚠️ Tree is empty, creating first leaf page.");
			leafPage = new LSHFBTLeafPage(headerPage.get_keyType());
			PageId newRootPageId = leafPage.getCurPage();

			leafPage.setNextPage(new PageId(INVALID_PAGE));
			leafPage.setPrevPage(new PageId(INVALID_PAGE));
			unpinPage(newRootPageId, true);
			updateHeader(newRootPageId);
			return;
		}

		// ✅ Step 2: Traverse down to the correct leaf page
		LSHFBTIndexPage indexPage;
		Page page = new Page();
		boolean done = false;

		
		String[] keys = bucketKey.split("_");
		String currentPath = keys[0];

		for(int i = 1; i < keys.length; i++) {

			currentPath +=  "_" + keys[i];

			StringKey pathKey = new StringKey(currentPath);

			//pinPage(currentPageId, page, false);
			Page currentPage = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(currentPage, headerPage.get_keyType()).getType();

			if (nodeType == NodeType.INDEX) {
				indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
				currentPageId = indexPage.getPageNoByKey(pathKey);
				unpinPage(indexPage.getCurPage(), false);
			} 
			else if (nodeType == NodeType.LEAF) {
				leafPage = new LSHFBTLeafPage(currentPage, AttrType.attrVector100D);
				break;
			}
		}

		// ✅ Step 3: Insert `<Vector100DKey, RID>` into the leaf node
		if (leafPage != null) {
			System.out.println("📌 Storing in Leaf: " + vectorKey  + " under path: " + bucketKey);
			
			//leafPage.clearPage();
			System.out.println("🔍 DEBUG: Leaf Page Record Count Before Insert: " + leafPage.numberOfRecords());

			leafPage.printLeafPageContents();

			leafPage.insertRecord(vectorKey, rid);
			unpinPage(leafPage.getCurPage(), true);
		} else {
			throw new InsertException(null, "Error finding correct leaf page.");
		}
	}

  
  
  /** insert record with the given key and rid
   *@param key the key of the record. Input parameter.
   *@param rid the rid of the record. Input parameter.
   *@exception  KeyTooLongException key size exceeds the max keysize.
   *@exception KeyNotMatchException key is not integer key nor string key
   *@exception IOException error from the lower layer
   *@exception LeafInsertRecException insert error in leaf page
   *@exception IndexInsertRecException insert error in index page
   *@exception ConstructPageException error in BT page constructor
   *@exception UnpinPageException error when unpin a page
   *@exception PinPageException error when pin a page
   *@exception NodeNotMatchException  node not match index page nor leaf page
   *@exception ConvertException error when convert between revord and byte 
   *             array
   *@exception DeleteRecException error when delete in index page
   *@exception IndexSearchException error when search 
   *@exception IteratorException iterator error
   *@exception LeafDeleteException error when delete in leaf page
   *@exception InsertException  error when insert in index page
   */    
  public void insert(KeyClass key, RID rid) 
    throws KeyTooLongException, 
	   KeyNotMatchException, 
	   LeafInsertRecException,   
	   IndexInsertRecException,
	   ConstructPageException, 
	   UnpinPageException,
	   PinPageException, 
	   NodeNotMatchException, 
	   ConvertException,
	   DeleteRecException,
	   IndexSearchException,
	   IteratorException, 
	   LeafDeleteException, 
	   InsertException,
	   IOException
	   
    {
      KeyDataEntry  newRootEntry;

	  System.out.println("Key Length: " + LSHFBT.getKeyLength(key));
      
      if (LSHFBT.getKeyLength(key) > headerPage.get_maxKeySize())
		throw new KeyTooLongException(null,"");
      
      if ( key instanceof StringKey ) {
		if ( headerPage.get_keyType() != AttrType.attrString ) {
			throw new KeyNotMatchException(null,"");
		}
      }
      else if ( key instanceof IntegerKey ) {
		if ( headerPage.get_keyType() != AttrType.attrInteger ) {
			throw new KeyNotMatchException(null,"");
		}
      }
	  else if (key instanceof Vector100DKey) {
		System.out.println("🔍 DEBUG: Vector100DKey detected, keyType in headerPage = " + headerPage.get_keyType());
		if ( headerPage.get_keyType() != AttrType.attrVector100D ) {
			throw new KeyNotMatchException(null,"");
		}
	  }   
      else 
		throw new KeyNotMatchException(null,"");
      
      
      // TWO CASES:
      // 1. headerPage.root == INVALID_PAGE:
      //    - the tree is empty and we have to create a new first page;
      //    this page will be a leaf page
      // 2. headerPage.root != INVALID_PAGE:
      //    - we call _insert() to insert the pair (key, rid)
      
      
      if ( trace != null )
	{
	  trace.writeBytes( "INSERT " + rid.pageNo + " "
			    + rid.slotNo + " " + key + lineSep);
	  trace.writeBytes( "DO" + lineSep);
	  trace.flush();
	}
      
      
      if (headerPage.get_rootId().pid == INVALID_PAGE) {
	PageId newRootPageId;
	LSHFBTLeafPage newRootPage;
	RID dummyrid;
	
	newRootPage=new LSHFBTLeafPage( headerPage.get_keyType());
	newRootPageId=newRootPage.getCurPage();
	
	
	if ( trace != null )
	  {
	    trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
	    trace.flush();
	  }
	
	
	
	newRootPage.setNextPage(new PageId(INVALID_PAGE));
	newRootPage.setPrevPage(new PageId(INVALID_PAGE));
	
	
	// ASSERTIONS:
	// - newRootPage, newRootPageId valid and pinned
	
	newRootPage.insertRecord(key, rid); 
	
	if ( trace!=null )
	  {
            trace.writeBytes("PUTIN node " + newRootPageId+lineSep);
            trace.flush();
	  }
	
	unpinPage(newRootPageId, true); /* = DIRTY */
	updateHeader(newRootPageId);
	
	if ( trace!=null )
	  {
            trace.writeBytes("DONE" + lineSep);
            trace.flush();
	  }
        
	
        return;
      }
      
      // ASSERTIONS:
      // - headerPageId, headerPage valid and pinned
      // - headerPage.root holds the pageId of the root of the B-tree
      // - none of the pages of the tree is pinned yet
      
      
      if ( trace != null )
	{
	  trace.writeBytes( "SEARCH" + lineSep);
	  trace.flush();
	}
      
      
      newRootEntry= _insert(key, rid, headerPage.get_rootId());
      
      // TWO CASES:
      // - newRootEntry != null: a leaf split propagated up to the root
      //                            and the root split: the new pageNo is in
      //                            newChildEntry.data.pageNo 
      // - newRootEntry == null: no new root was created;
      //                            information on headerpage is still valid
      
      // ASSERTIONS:
      // - no page pinned
      
      if (newRootEntry != null)
	{
	  LSHFBTIndexPage newRootPage;
	  PageId      newRootPageId;
	  Object      newEntryKey;
	  
	  // the information about the pair <key, PageId> is
	  // packed in newRootEntry: extract it
	  
	  newRootPage = new LSHFBTIndexPage(headerPage.get_keyType());
	  newRootPageId=newRootPage.getCurPage();
	  
	  // ASSERTIONS:
	  // - newRootPage, newRootPageId valid and pinned
	  // - newEntryKey, newEntryPage contain the data for the new entry
	  //     which was given up from the level down in the recursion
	  
	  
	  if ( trace != null )
	    {
	      trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
	      trace.flush();
	    }
	  
	  
	  newRootPage.insertKey( newRootEntry.key, 
				 ((IndexData)newRootEntry.data).getData() );
	  
	  
	  // the old root split and is now the left child of the new root
	  newRootPage.setPrevPage(headerPage.get_rootId());
	  
	  unpinPage(newRootPageId, true /* = DIRTY */);
	  
	  updateHeader(newRootPageId);
	  
	}
      
      
      if ( trace !=null )
	{
	  trace.writeBytes("DONE"+lineSep);
	  trace.flush();
	}
      
      
      return;
    }
  
  
  
  
  private KeyDataEntry  _insert(KeyClass key, RID rid,  
				PageId currentPageId) 
    throws  PinPageException,  
	    IOException,
	    ConstructPageException, 
	    LeafDeleteException,  
	    ConstructPageException,
	    DeleteRecException, 
	    IndexSearchException,
	    UnpinPageException, 
	    LeafInsertRecException,
	    ConvertException, 
	    IteratorException, 
	    IndexInsertRecException,
	    KeyNotMatchException, 
	    NodeNotMatchException,
	    InsertException 
	    
    {
      
      
      LSHFBTSortedPage currentPage;
      Page page;
      KeyDataEntry upEntry;
      
      
      page=pinPage(currentPageId);
      currentPage=new LSHFBTSortedPage(page, headerPage.get_keyType());      
      
      if ( trace!=null )
	{
	  trace.writeBytes("VISIT node " + currentPageId+lineSep);
	  trace.flush();
	}
      
      
      // TWO CASES:
      // - pageType == INDEX:
      //   recurse and then split if necessary
      // - pageType == LEAF:
      //   try to insert pair (key, rid), maybe split
      
      if(currentPage.getType() == NodeType.INDEX) {
	LSHFBTIndexPage  currentIndexPage=new LSHFBTIndexPage(page, 
						      headerPage.get_keyType());
	PageId       currentIndexPageId = currentPageId;
	PageId nextPageId;
	
	nextPageId=currentIndexPage.getPageNoByKey(key);
	
	// now unpin the page, recurse and then pin it again
	unpinPage(currentIndexPageId);
	
	upEntry= _insert(key, rid, nextPageId);
	
	// two cases:
	// - upEntry == null: one level lower no split has occurred:
	//                     we are done.
	// - upEntry != null: one of the children has split and
	//                    upEntry is the new data entry which has
	//                    to be inserted on this index page
	
	if ( upEntry == null)
	  return null;
	
	currentIndexPage= new  LSHFBTIndexPage(pinPage(currentPageId),
					   headerPage.get_keyType() );
	
	// ASSERTIONS:
	// - upEntry != null
	// - currentIndexPage, currentIndexPageId valid and pinned
	
	// the information about the pair <key, PageId> is
	// packed in upEntry
	
	// check whether there can still be entries inserted on that page
	if (currentIndexPage.available_space() >=
	    LSHFBT.getKeyDataLength( upEntry.key, NodeType.INDEX))
	  {
	    
	    // no split has occurred
	    currentIndexPage.insertKey( upEntry.key, 
					((IndexData)upEntry.data).getData() );
	    
	    unpinPage(currentIndexPageId, true /* DIRTY */);
	    
	    return null;
	  }
	
	// ASSERTIONS:
	// - on the current index page is not enough space available . 
	//   it splits
	
	//   therefore we have to allocate a new index page and we will
	//   distribute the entries
	// - currentIndexPage, currentIndexPageId valid and pinned
	
	LSHFBTIndexPage newIndexPage;
	PageId       newIndexPageId;
	
	// we have to allocate a new INDEX page and
	// to redistribute the index entries
	newIndexPage= new LSHFBTIndexPage(headerPage.get_keyType());
	newIndexPageId=newIndexPage.getCurPage();  
	
        
	if ( trace !=null )
	  {
	    if (headerPage.get_rootId().pid != currentIndexPageId.pid) 
	      trace.writeBytes("SPLIT node " + currentIndexPageId 
			       + " IN nodes " +  currentIndexPageId +
			       " " + newIndexPageId +lineSep);
	    else
	      trace.writeBytes("ROOTSPLIT IN nodes " + currentIndexPageId 
			       + " " +  newIndexPageId +lineSep);
	    trace.flush();
	  }
	
	
	// ASSERTIONS:
	// - newIndexPage, newIndexPageId valid and pinned
	// - currentIndexPage, currentIndexPageId valid and pinned
	// - upEntry containing (Key, Page) for the new entry which was
	//     given up from the level down in the recursion
	
	KeyDataEntry      tmpEntry;
	PageId       tmpPageId;
	RID insertRid;
	RID delRid=new RID();
	
	for ( tmpEntry= currentIndexPage.getFirst( delRid);
	      tmpEntry!=null;tmpEntry= currentIndexPage.getFirst( delRid))  
	  {
	    newIndexPage.insertKey( tmpEntry.key, 
				    ((IndexData)tmpEntry.data).getData());
	    currentIndexPage.deleteSortedRecord(delRid);
	  }
	
	// ASSERTIONS:
	// - currentIndexPage empty
	// - newIndexPage holds all former records from currentIndexPage
	
	// we will try to make an equal split
	RID firstRid=new RID();
	KeyDataEntry undoEntry=null;
	for (tmpEntry = newIndexPage.getFirst(firstRid);
	     (currentIndexPage.available_space() >
	      newIndexPage.available_space());
	     tmpEntry=newIndexPage.getFirst(firstRid))
	  {
	    // now insert the <key,pageId> pair on the new
	    // index page
	    undoEntry=tmpEntry;
	    currentIndexPage.insertKey( tmpEntry.key, 
					((IndexData)tmpEntry.data).getData());
	    newIndexPage.deleteSortedRecord(firstRid);
          }
	
	//undo the final record
	if ( currentIndexPage.available_space() < 
	     newIndexPage.available_space()) {
	  
	  newIndexPage.insertKey( undoEntry.key, 
				  ((IndexData)undoEntry.data).getData());
	  
	  currentIndexPage.deleteSortedRecord 
	    (new RID(currentIndexPage.getCurPage(),
		     (int)currentIndexPage.getSlotCnt()-1) );              
	}
	
	
	
	// check whether <newKey, newIndexPageId>
	// will be inserted
	// on the newly allocated or on the old index page
	
	tmpEntry= newIndexPage.getFirst(firstRid);
	
	if (LSHFBT.keyCompare( upEntry.key, tmpEntry.key) >=0 )
	  {
	    // the new data entry belongs on the new index page
	    newIndexPage.insertKey( upEntry.key, 
				    ((IndexData)upEntry.data).getData());
          }
	else {
	  currentIndexPage.insertKey( upEntry.key, 
				      ((IndexData)upEntry.data).getData());
	  
	  int i= (int)currentIndexPage.getSlotCnt()-1;
	  tmpEntry =
	    LSHFBT.getEntryFromBytes(currentIndexPage.getpage(), 
				 currentIndexPage.getSlotOffset(i),
				 currentIndexPage.getSlotLength(i),
				 headerPage.get_keyType(),NodeType.INDEX);

	  newIndexPage.insertKey( tmpEntry.key, 
				  ((IndexData)tmpEntry.data).getData());

	  currentIndexPage.deleteSortedRecord
	    (new RID(currentIndexPage.getCurPage(), i) );      
	  
	}
	
	
	  unpinPage(currentIndexPageId, true /* dirty */);
        
	  // fill upEntry
	  upEntry= newIndexPage.getFirst(delRid);

	  // now set prevPageId of the newIndexPage to the pageId
	  // of the deleted entry:
	  newIndexPage.setPrevPage( ((IndexData)upEntry.data).getData());

	  // delete first record on new index page since it is given up
	  newIndexPage.deleteSortedRecord(delRid);
	  
	  unpinPage(newIndexPageId, true /* dirty */);
	  
	  
	  if ( trace!=null ){
	    trace_children(currentIndexPageId);
	    trace_children(newIndexPageId);
	  }
	  
	  
          ((IndexData)upEntry.data).setData( newIndexPageId);
	  
          return upEntry;  
	  
	  // ASSERTIONS:
	  // - no pages pinned
	  // - upEntry holds the pointer to the KeyDataEntry which is
	  //   to be inserted on the index page one level up
	  
      }
      
      else if ( currentPage.getType()==NodeType.LEAF)
	{
	  LSHFBTLeafPage currentLeafPage = 
	    new LSHFBTLeafPage(page, headerPage.get_keyType() );

	  PageId currentLeafPageId = currentPageId;
	  
	  // ASSERTIONS:
	  // - currentLeafPage, currentLeafPageId valid and pinned
	  
	  // check whether there can still be entries inserted on that page
	  if (currentLeafPage.available_space() >=
	      LSHFBT.getKeyDataLength(key, NodeType.LEAF) )
	    {
	      // no split has occurred
	      
	      currentLeafPage.insertRecord(key, rid); 
	      
	      unpinPage(currentLeafPageId, true /* DIRTY */);
	      
	      
	      if ( trace !=null )
		{
		  trace.writeBytes("PUTIN node "+ currentLeafPageId+lineSep);
		  trace.flush();
		}
	      
	      
	      return null;
	    }
	  
	  // ASSERTIONS:
	  // - on the current leaf page is not enough space available. 
	  //   It splits.
	  // - therefore we have to allocate a new leaf page and we will
	  // - distribute the entries
	  
	  LSHFBTLeafPage  newLeafPage;
	  PageId       newLeafPageId;
	  // we have to allocate a new LEAF page and
	  // to redistribute the data entries entries
	  newLeafPage=new LSHFBTLeafPage(headerPage.get_keyType());
	  newLeafPageId=newLeafPage.getCurPage();
	  
	  newLeafPage.setNextPage(currentLeafPage.getNextPage());
	  newLeafPage.setPrevPage(currentLeafPageId);  // for dbl-linked list
	  currentLeafPage.setNextPage(newLeafPageId);
	  
	  // change the prevPage pointer on the next page:
	  
	  PageId rightPageId;
	  rightPageId = newLeafPage.getNextPage();
	  if (rightPageId.pid != INVALID_PAGE)
	    {
	      LSHFBTLeafPage rightPage;
	      rightPage=new LSHFBTLeafPage(rightPageId, headerPage.get_keyType());
	      
	      rightPage.setPrevPage(newLeafPageId);
	      unpinPage(rightPageId, true /* = DIRTY */);
	      
	      // ASSERTIONS:
	      // - newLeafPage, newLeafPageId valid and pinned
	      // - currentLeafPage, currentLeafPageId valid and pinned
	    }
	  
	  if ( trace!=null )
	    {
	      if (headerPage.get_rootId().pid != currentLeafPageId.pid) 
		trace.writeBytes("SPLIT node " + currentLeafPageId 
				 + " IN nodes "
				 + currentLeafPageId + " " + newLeafPageId +lineSep);
	      else
		trace.writeBytes("ROOTSPLIT IN nodes " + currentLeafPageId
				 + " " +  newLeafPageId +lineSep);
	      trace.flush();
	    }
	  
	  
	  
	  KeyDataEntry     tmpEntry;
	  RID       firstRid=new RID();
	  
	  
	  for (tmpEntry = currentLeafPage.getFirst(firstRid);
	       tmpEntry != null;
	       tmpEntry = currentLeafPage.getFirst(firstRid))
	    {
	      
	      newLeafPage.insertRecord( tmpEntry.key,
					((LeafData)(tmpEntry.data)).getData());
	      currentLeafPage.deleteSortedRecord(firstRid);
	      
	    }
	  
	  
	  // ASSERTIONS:
	  // - currentLeafPage empty
	  // - newLeafPage holds all former records from currentLeafPage
	  
	  KeyDataEntry undoEntry=null; 
	  for (tmpEntry = newLeafPage.getFirst(firstRid);
	       newLeafPage.available_space() < 
		 currentLeafPage.available_space(); 
               tmpEntry=newLeafPage.getFirst(firstRid)   )
	    {	   
	      undoEntry=tmpEntry;
	      currentLeafPage.insertRecord( tmpEntry.key,
					    ((LeafData)tmpEntry.data).getData());
	      newLeafPage.deleteSortedRecord(firstRid);		
	    }
	  
	  if (LSHFBT.keyCompare(key, undoEntry.key ) <  0) {
	    //undo the final record
	    if ( currentLeafPage.available_space() < 
		 newLeafPage.available_space()) {
	      newLeafPage.insertRecord( undoEntry.key, 
					((LeafData)undoEntry.data).getData());
	      
	      currentLeafPage.deleteSortedRecord
		(new RID(currentLeafPage.getCurPage(),
			 (int)currentLeafPage.getSlotCnt()-1) );              
	    }
	  }	  
	  
	  // check whether <key, rid>
	  // will be inserted
	  // on the newly allocated or on the old leaf page
	  
	  if (LSHFBT.keyCompare(key,undoEntry.key ) >= 0)
	    {                     
	      // the new data entry belongs on the new Leaf page
	      newLeafPage.insertRecord(key, rid);
	      
	      
	      if ( trace!=null )
		{
		  trace.writeBytes("PUTIN node " + newLeafPageId +lineSep);
		  trace.flush();
		}
	      
	      
	    }
	  else {
	    currentLeafPage.insertRecord(key,rid);
	  }
	  
	  unpinPage(currentLeafPageId, true /* dirty */);
	  
	  if ( trace!=null ){
	    trace_children(currentLeafPageId);
	    trace_children(newLeafPageId);
	  }
	  
	  
	  
	  // fill upEntry
	  tmpEntry=newLeafPage.getFirst(firstRid);
	  upEntry=new KeyDataEntry(tmpEntry.key, newLeafPageId );
	  
	  
	  unpinPage(newLeafPageId, true /* dirty */);
	  
	  // ASSERTIONS:
	  // - no pages pinned
	  // - upEntry holds the valid KeyDataEntry which is to be inserted 
	  // on the index page one level up
	  return upEntry;
	}
      else {    
	throw new InsertException(null,"");
      }
    }
  
  
  
  
  
  
  
  
  /* 
   * findRunStart.
   * Status BTreeFile::findRunStart (const void   lo_key,
   *                                RID          *pstartrid)
   *
   * find left-most occurrence of `lo_key', going all the way left if
   * lo_key is null.
   * 
   * Starting record returned in *pstartrid, on page *pppage, which is pinned.
   *
   * Since we allow duplicates, this must "go left" as described in the text
   * (for the search algorithm).
   *@param lo_key  find left-most occurrence of `lo_key', going all 
   *               the way left if lo_key is null.
   *@param startrid it will reurn the first rid =< lo_key
   *@return return a LSHFBTLeafPage instance which is pinned. 
   *        null if no key was found.
   */
  
  LSHFBTLeafPage findRunStart (KeyClass lo_key, 
			   RID startrid)
    throws IOException, 
	   IteratorException,  
	   KeyNotMatchException,
	   ConstructPageException, 
	   PinPageException, 
	   UnpinPageException
    {
      LSHFBTLeafPage  pageLeaf;
      LSHFBTIndexPage pageIndex;
      Page page;
      LSHFBTSortedPage  sortPage;
      PageId pageno;
      PageId curpageno=null;                // iterator
      PageId prevpageno;
      PageId nextpageno;
      RID curRid;
      KeyDataEntry curEntry;
      
      pageno = headerPage.get_rootId();
      
      if (pageno.pid == INVALID_PAGE){        // no pages in the BTREE
        pageLeaf = null;                // should be handled by 
        // startrid =INVALID_PAGEID ;             // the caller
        return pageLeaf;
      }
      
      page= pinPage(pageno);
      sortPage=new LSHFBTSortedPage(page, headerPage.get_keyType());
      
      
      if ( trace!=null ) {
	trace.writeBytes("VISIT node " + pageno + lineSep);
	trace.flush();
      }
      
      
      // ASSERTION
      // - pageno and sortPage is the root of the btree
      // - pageno and sortPage valid and pinned
      
      while (sortPage.getType() == NodeType.INDEX) {
	pageIndex=new LSHFBTIndexPage(page, headerPage.get_keyType()); 
	prevpageno = pageIndex.getPrevPage();
	curEntry= pageIndex.getFirst(startrid);
	while ( curEntry!=null && lo_key != null 
		&& LSHFBT.keyCompare(curEntry.key, lo_key) < 0) {
	  
          prevpageno = ((IndexData)curEntry.data).getData();
          curEntry=pageIndex.getNext(startrid);
	}
	
	unpinPage(pageno);
	
	pageno = prevpageno;
	page=pinPage(pageno);
	sortPage=new LSHFBTSortedPage(page, headerPage.get_keyType()); 
	
	
	if ( trace!=null )
	  {
	    trace.writeBytes( "VISIT node " + pageno+lineSep);
	    trace.flush();
	  }
	
	
      }
      
      pageLeaf = new LSHFBTLeafPage(page, headerPage.get_keyType() );
      
      curEntry=pageLeaf.getFirst(startrid);
      while (curEntry==null) {
	// skip empty leaf pages off to left
	nextpageno = pageLeaf.getNextPage();
	unpinPage(pageno);
	if (nextpageno.pid == INVALID_PAGE) {
	  // oops, no more records, so set this scan to indicate this.
	  return null;
	}
	
	pageno = nextpageno; 
	pageLeaf=  new LSHFBTLeafPage( pinPage(pageno), headerPage.get_keyType());    
	curEntry=pageLeaf.getFirst(startrid);
      }
      
      // ASSERTIONS:
      // - curkey, curRid: contain the first record on the
      //     current leaf page (curkey its key, cur
      // - pageLeaf, pageno valid and pinned
      
      
      if (lo_key == null) {
	return pageLeaf;
	// note that pageno/pageLeaf is still pinned; 
	// scan will unpin it when done
      }
      
      while (LSHFBT.keyCompare(curEntry.key, lo_key) < 0) {
	curEntry= pageLeaf.getNext(startrid);
	while (curEntry == null) { // have to go right
	  nextpageno = pageLeaf.getNextPage();
	  unpinPage(pageno);
	  
	  if (nextpageno.pid == INVALID_PAGE) {
	    return null;
	  }
	  
	  pageno = nextpageno;
	  pageLeaf=new LSHFBTLeafPage(pinPage(pageno), headerPage.get_keyType());
	  
	  curEntry=pageLeaf.getFirst(startrid);
	}
      }
      
      return pageLeaf;
    }
  
	public boolean Delete(KeyClass key, RID rid) {
		throw new UnsupportedOperationException("Delete not implemented yet.");
	}
 
  
  
  
  /** create a scan with given keys
   * Cases:
   *      (1) lo_key = null, hi_key = null
   *              scan the whole index
   *      (2) lo_key = null, hi_key!= null
   *              range scan from min to the hi_key
   *      (3) lo_key!= null, hi_key = null
   *              range scan from the lo_key to max
   *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
   *              exact match ( might not unique)
   *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
   *              range scan from lo_key to hi_key
   *@param lo_key the key where we begin scanning. Input parameter.
   *@param hi_key the key where we stop scanning. Input parameter.
   *@exception IOException error from the lower layer
   *@exception KeyNotMatchException key is not integer key nor string key
   *@exception IteratorException iterator error
   *@exception ConstructPageException error in BT page constructor
   *@exception PinPageException error when pin a page
   *@exception UnpinPageException error when unpin a page
   */
  public LSHFBTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
    throws IOException,  
	   KeyNotMatchException, 
	   IteratorException, 
	   ConstructPageException, 
	   PinPageException, 
	   UnpinPageException
	   
    {
      LSHFBTFileScan scan = new LSHFBTFileScan();
      if ( headerPage.get_rootId().pid==INVALID_PAGE) {
	scan.leafPage=null;
	return scan;
      }
      
      scan.treeFilename=dbname;
      scan.endkey=hi_key;
      scan.didfirst=false;
      scan.deletedcurrent=false;
      scan.curRid=new RID();     
      scan.keyType=headerPage.get_keyType();
      scan.maxKeysize=headerPage.get_maxKeySize();
      scan.bfile=this;
      
      //this sets up scan at the starting position, ready for iteration
      scan.leafPage=findRunStart( lo_key, scan.curRid);
      return scan;
    }
  
  void trace_children(PageId id)
    throws  IOException, 
	    IteratorException, 
	    ConstructPageException,
	    PinPageException, 
	    UnpinPageException
    {
      
      if( trace!=null ) {
	
	LSHFBTSortedPage sortedPage;
	RID metaRid=new RID();
	PageId childPageId;
	KeyClass key;
	KeyDataEntry entry;
	sortedPage=new LSHFBTSortedPage( pinPage( id), headerPage.get_keyType());
	
	
	// Now print all the child nodes of the page.  
	if( sortedPage.getType()==NodeType.INDEX) {
	  LSHFBTIndexPage indexPage=new LSHFBTIndexPage(sortedPage,headerPage.get_keyType()); 
	  trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
	  trace.writeBytes( " " + indexPage.getPrevPage());
	  for ( entry = indexPage.getFirst( metaRid );
		entry != null;
		entry = indexPage.getNext( metaRid ) )
	    {
	      trace.writeBytes( "   " + ((IndexData)entry.data).getData());
	    }
	}
	else if( sortedPage.getType()==NodeType.LEAF) {
	  LSHFBTLeafPage leafPage=new LSHFBTLeafPage(sortedPage,headerPage.get_keyType()); 
	  trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
	  for ( entry = leafPage.getFirst( metaRid );
		entry != null;
		entry = leafPage.getNext( metaRid ) )
	    {
	      trace.writeBytes( "   " + entry.key + " " + entry.data);
	    }
	}
	unpinPage( id );
	trace.writeBytes(lineSep);
	trace.flush();
      }
      
    }
  
}


