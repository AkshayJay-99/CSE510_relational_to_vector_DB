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
 
 import java.util.ArrayList;
 import java.util.HashSet;
 
 import java.util.Queue;
 import java.util.LinkedList;
 
 // for debug
 import java.util.HashMap;
 
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
 
   //private Set<Integer> leafPageIds = new HashSet<>();
 
 
 // for debug
	 private static HashMap<Integer, Integer> pinCountMap = new HashMap<>();
   
   
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
   
   public PageId get_file_entry(String filename)         
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
   
   
   
   public Page pinPage(PageId pageno) 
	 throws PinPageException
	 {
	   try {
		 Page page=new Page();
		 SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
 
		 pinCountMap.put(pageno.pid, pinCountMap.getOrDefault(pageno.pid, 0) + 1);
 
		 //System.out.println("📌 PINNED Page: " + pageno.pid + " (Count: " + pinCountMap.get(pageno.pid) + ")");
 
		 // 🔎 Log stack trace if this page is pinned excessively
 
		 // if (pageno.pid == 10) {
		 //     System.out.println("🚨 Page 10 is being pinned HERE! Stack Trace:");
		 //     new Exception().printStackTrace();
		 // }
 
		 // if (pinCountMap.get(pageno.pid) > 0) {  
		 // 	Exception e = new Exception();
		 // 	System.out.println("🔎 STACK TRACE for excessive PIN on Page: " + pageno.pid);
		 // 	e.printStackTrace(System.out);
		 // }
 
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
   
   public void unpinPage(PageId pageno) 
	 throws UnpinPageException
	 { 
	   try{
		 
		 SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
		 
		 pinCountMap.put(pageno.pid, pinCountMap.getOrDefault(pageno.pid, 0) - 1);
 
		 // if (pageno.pid == 10) {
		 //     System.out.println("🚨 Page 10 is being unpinned HERE! Stack Trace:");
		 //     new Exception().printStackTrace();
		 // }
 
		 //System.out.println("✅ UNPINNED Page: " + pageno.pid + " (Remaining: " + pinCountMap.get(pageno.pid) + ")");
 
		 // 🔎 Log stack trace if a page remains pinned too many times
		 // if (pinCountMap.get(pageno.pid) > 0) {  
		 //     Exception e = new Exception();
		 //     System.out.println("🔎 STACK TRACE for excessive UNPIN on Page: " + pageno.pid);
		 //     e.printStackTrace(System.out);
		 // }
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
   
   public void unpinPage(PageId pageno, boolean dirty) 
	 throws UnpinPageException
	 {
	   try{
		 SystemDefs.JavabaseBM.unpinPage(pageno, dirty); 
 
		 
		 pinCountMap.put(pageno.pid, pinCountMap.getOrDefault(pageno.pid, 0) - 1);
 
		 // if (pageno.pid == 10) {
		 //     System.out.println("🚨 Page 10 is being unpinned HERE! Stack Trace:");
		 //     new Exception().printStackTrace();
		 // }
 
		 //System.out.println("✅ UNPINNED Page: " + pageno.pid + " (Remaining: " + pinCountMap.get(pageno.pid) + ")");
 
		 // // 🔎 Log stack trace if a page remains pinned too many times
		 // if (pinCountMap.get(pageno.pid) > 0) {  
		 //     Exception e = new Exception();
		 //     System.out.println("🔎 STACK TRACE for excessive UNPIN on Page: " + pageno.pid);
		 //     e.printStackTrace(System.out);
		 // }
		 
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
		ConstructPageException,
		ReplacerException,
		PageUnpinnedException,
		HashEntryNotFoundException,
		InvalidFrameNumberException    
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
		AddFileEntryException,
		ReplacerException,
		PageUnpinnedException,
		HashEntryNotFoundException,
		InvalidFrameNumberException 
	 {
	   
	   
	   headerPageId=get_file_entry(filename);
 
	 //   if (headerPageId == null) {
	 // 		System.out.println("❌ No file entry found for: " + filename);
	 // 	} else {
	 // 		System.out.println("✅ Found existing file entry for: " + filename + " -> PageId: " + headerPageId);
	 // 	}
 
	   if( headerPageId==null) //file not exist
	 {
	   headerPage= new  LSHFHeaderPage(); 
	   headerPageId= headerPage.getPageId();
	   add_file_entry(filename, headerPageId);
	   //System.out.println("📌 Added file entry for: " + filename + " -> PageId: " + headerPageId);
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
 
	   //System.out.println("header id: " + headerPageId);
	   try {
			 SystemDefs.JavabaseBM.unpinPage(new PageId(0), true);
			 //System.out.println(" Forced unpin for Page 0 at startup");
		 } catch (Exception e) {
			 //System.out.println(" Could not unpin Page 0 at startup: " + e.getMessage());
		 }
	   
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
			ReplacerException,
			HashOperationException,
			PagePinnedException,
			PageNotFoundException,
			BufMgrException,
			IOException
	 {
		 //System.out.println("🔄 Attempting to unpin and close B+ Tree index...");
 
		 //System.out.println("🔧 Applying TEMP FIX for BTree_Layer0: Unpinning all tracked pages.");
		 for (Integer pid : pinCountMap.keySet()) {
			 try {
				 SystemDefs.JavabaseBM.unpinPage(new PageId(pid), true);
				 //System.out.println("✅ [TEMP FIX] Forced unpin for Page: " + pid);
			 } catch (Exception e) {
				 //System.out.println("❌ [TEMP FIX FAILED] Could not unpin Page: " + pid);
			 }
		 }
 
		 // System.out.println("🔎 Debug: Printing all stored page IDs before closing...");
		 // for (int i = 0; i < SystemDefs.JavabaseBM.getNumBuffers(); i++) {
		 // 	PageId pid = new PageId(i);
		 // 	System.out.println("📌 Page " + pid.pid + " should be saved.");
		 // }
 
 
		 // 🔍 Step 2: Log any still-pinned pages
		 //System.out.println("🔍 Checking for still-pinned pages BEFORE flushing...");
 
		 int remainingPinnedCount = 0;
		 for (int i = 0; i < SystemDefs.JavabaseBM.getNumBuffers(); i++) {
			 PageId pid = new PageId(i);
			 try {
				 SystemDefs.JavabaseBM.unpinPage(pid, true);
				 remainingPinnedCount++;
				 //System.out.println("⚠️ WARNING: Still pinned Page: " + pid.pid + " (Index 0)");
			 } catch (PageUnpinnedException | HashEntryNotFoundException ignored) {
				 // ✅ Ignore already unpinned/missing pages
			 }
		 }
 
		 
 
		 // System.out.println("🔄 Ensuring all leaf pages are unpinned before closing...");
		 // for (Integer pid : leafPageIds) { // <-- Track leaf pages in a set during insert
		 // 	try {
		 // 		SystemDefs.JavabaseBM.unpinPage(new PageId(pid), true);
		 // 		System.out.println("✅ Unpinned Leaf Page: " + pid);
		 // 	} catch (Exception e) {
		 // 		System.out.println("❌ ERROR: Could not unpin Leaf Page: " + pid);
		 // 	}
		 // }
 
		 // ✅ Step 1: Unpin Header Page
		 if (headerPageId != null) {
			 try {
				 SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
				 //System.out.println("✅ Header page unpinned successfully.");
			 } catch (PageUnpinnedException | HashEntryNotFoundException e) {
				 //System.out.println("⚠️ WARNING: Header page not found in buffer pool (already unpinned?)");
			 }
		 }
 
		 
		 //SystemDefs.JavabaseBM.flushAllPages();
		 // ✅ Step 3: Flush Pages Only If No Pinned Pages Remain
		 if (remainingPinnedCount == 0) {
			 SystemDefs.JavabaseBM.flushAllPages();
			 //System.out.println("✅ B+ Tree Index closed and all pages flushed.");
		 } else {
			 //System.out.println("❌ ERROR: " + remainingPinnedCount + " pages are still pinned! Investigate further.");
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
			 entry!=null;
			 entry = indexPage.getNext(rid))
		 { 
			 childId = ((IndexData)(entry.data)).getData();
			 _destroyFile(childId);
		 }
	   }
	 
		 unpinPage(pageno);
		 freePage(pageno);
 
	   //unpinPage(pageno);
	   
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
 
	 public ArrayList<KeyDataEntry> NNSearch(String bucketKey, int number_of_neighbors) 
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
		 
		 //System.out.println("🔍 Target Path: " + bucketKey);
 
		 // ✅ Step 1: Start from the root
		 PageId currentPageId = headerPage.get_rootId();
		 LSHFBTLeafPage leafPage = null;
		 LSHFBTIndexPage indexPage = null;
 
		 ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
		 ArrayList<PageId> parentNodes = new ArrayList<>();
		 ArrayList<PageId> visitedLeaf = new ArrayList<>();
 
		 if (currentPageId.pid == INVALID_PAGE) {
			 System.out.println("⚠️ Tree is empty, creating first leaf page.");
 
			 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
			 PageId newRootPageId = leafPage.getCurPage();
 
			 leafPage.setNextPage(new PageId(INVALID_PAGE));
			 leafPage.setPrevPage(new PageId(INVALID_PAGE));
 
			 System.out.println("✅ Created new ROOT Leaf Node at Page ID: " + newRootPageId.pid);
 
			 unpinPage(newRootPageId, true);
			 updateHeader(newRootPageId);
			 return nearestNeighbors;
		 }
 
		 // ✅ Step 2: Traverse to the last internal node before creating a leaf
		 Page page;
		 String[] keys = bucketKey.split("_");
		 String currentPath = keys[0];
 
		 for (int i = 1; i < keys.length; i++) {
			 currentPath += "_" + keys[i];
			 StringKey pathKey = new StringKey(currentPath);
			 page = pinPage(currentPageId);
			 short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();
			 
			 // System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);
			 // System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
 
			 if (nodeType == NodeType.INDEX) {
				 indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
				 parentNodes.add(currentPageId);
				 PageId nextPageId = indexPage.getPageNoByKey(pathKey);
 
				 // ✅ **We are at the last step of traversal** - determine leaf creation vs. reference
				 if (i == keys.length - 1) {
					 if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
						 // ✅ No leaf exists, create a new one
						 //System.out.println("⚠️ No leaf found at: " + currentPath + " -> Creating new one.");
						 
						 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
						 PageId leafPageId = leafPage.getCurPage();
						 
						 //System.out.println("✅ New Leaf Created at Page ID: " + leafPageId.pid);
						 
						 // ✅ Link the new leaf to the parent index
						 indexPage.insertKey(pathKey, leafPageId);
 
						 //System.out.println("🔎 DEBUG: Verifying Parent Index After Inserting Leaf...");
						 PageId verifyPage = indexPage.getPageNoByKey(pathKey);
						 // if (verifyPage == null || verifyPage.pid == INVALID_PAGE) {
						 // 	System.out.println("❌ ERROR: Parent index did NOT correctly store reference to new leaf!");
						 // } else {
						 // 	System.out.println("✅ Parent index correctly references new leaf at Page ID: " + verifyPage.pid);
						 // }
 
						 // 🚀 Ensure it was inserted properly
						 PageId checkPage = indexPage.getPageNoByKey(pathKey);
						 if (checkPage == null || checkPage.pid == INVALID_PAGE) {
							 //System.out.println("❌ ERROR: Failed to properly link leaf page to index!");
							 throw new InsertException(null, "Leaf was created but not linked properly.");
						 }
 
						 //System.out.println("✅ SUCCESS: Leaf correctly linked at Page ID: " + checkPage.pid);
 
						 // ✅ Reference this for insertion in Step 3
						 currentPageId = leafPageId;
						 unpinPage(currentPageId, true);
						 
					 } else {
						 // ✅ Leaf already exists - reference it for insertion
						 // System.out.println("➡️ Found node at: " + currentPath + " | Current Page ID: " + currentPageId.pid);
						 // System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
						 // System.out.println("✅ Found existing leaf at: " + currentPath);
						 
						 Page nextPage = pinPage(nextPageId);
						 short nextNodeType = new LSHFBTSortedPage(nextPage, headerPage.get_keyType()).getType();
						 
						 if (nextNodeType == NodeType.LEAF) {
							 // ✅ Correctly reference the existing leaf page
							 leafPage = new LSHFBTLeafPage(nextPage, AttrType.attrVector100D);
							 //System.out.println("✅ Confirmed existing leaf at: " + currentPath + " (Page ID: " + nextPageId.pid + ")");
							 unpinPage(nextPageId, true);
 
						 } else {
							 // 🚨 It's actually an index, so we need to create a new leaf instead
							 //System.out.println("⚠️ WARNING: Expected leaf at " + currentPath + ", but found an INDEX instead! Creating a new leaf.");
							 
							 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
							 PageId newLeafPageId = leafPage.getCurPage();
							 
							 // ✅ Link the new leaf to the index page
							 //indexPage.insertKey(pathKey, newLeafPageId);
							 //System.out.println("🔗 Linking new leaf to parent index: " + currentPath + " (Page ID: " + newLeafPageId.pid + ")");
							 indexPage.insertKey(pathKey, newLeafPageId);
 
							 //System.out.println("🔎 DEBUG: Verifying Parent Index After Inserting Leaf...");
							 PageId verifyPage = indexPage.getPageNoByKey(pathKey);
							 // if (verifyPage == null || verifyPage.pid == INVALID_PAGE) {
							 // 	System.out.println("❌ ERROR: Parent index did NOT correctly store reference to new leaf!");
							 // } else {
							 // 	System.out.println("✅ Parent index correctly references new leaf at Page ID: " + verifyPage.pid);
							 // }
												 
							 //System.out.println("✅ SUCCESS: Leaf correctly linked at Page ID: " + verifyPage.pid);
							 currentPageId = newLeafPageId;
							 
							 // ✅ Unpin the newly created leaf so it gets written to disk
							 unpinPage(newLeafPageId, true);
						 }
					 }
 
					 unpinPage(indexPage.getCurPage(), true);
					 break;  // **Exit loop - we found or created the leaf**
				 }
				 unpinPage(currentPageId);  
				 currentPageId = nextPageId;
				 
			 }
		 }
		 //System.out.println("is leaf page null? " + leafPage.getCurPage());
 
		 // ✅ Step 3: Insert the record into the found or newly created leaf
		 while (leafPage != null) {
			 int recordCount = 0;
			 RID countRid = new RID();
			 KeyDataEntry countEntry = leafPage.getFirst(countRid);
			 while (countEntry != null) {
				 recordCount++;
				 countEntry = leafPage.getNext(countRid);
			 }
			 //System.out.println("📊 DEBUG: Total Records in Leaf Page " + leafPage.getCurPage().pid + " = " + recordCount);
 
			 // ✅ Process and store leaf entries
			 pinPage(leafPage.getCurPage());
			 RID rid = new RID();
			 KeyDataEntry entry = leafPage.getFirst(rid);
			 while (entry != null) {
				 //System.out.println("✅ Leaf Record: " + entry.key);
				 nearestNeighbors.add(entry);  // ✅ Add to nearest neighbors list
				 //if (nearestNeighbors.size() >= number_of_neighbors) break; // Stop when we have enough
				 entry = leafPage.getNext(rid);
			 }
			 unpinPage(leafPage.getCurPage(), true);
 
			 // ✅ Check for right sibling before moving up
			 PageId rightSiblingId = leafPage.getNextPage();
 
			 // if (rightSiblingId.pid != INVALID_PAGE) {
			 // 	//System.out.println("➡️ Moving to existing right sibling: Page " + rightSiblingId.pid);
 
			 // 	if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
			 // 		unpinPage(leafPage.getCurPage(), false);
			 // 	}
 
			 // 	leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
			 // 	continue;
			 // }
 
			 while (rightSiblingId.pid != INVALID_PAGE) { // Keep traversing all right siblings
				 //System.out.println("➡️ Moving to right sibling: Page " + rightSiblingId.pid);
				 //System.out.println("📊 DEBUG: Total Records in Leaf Page " + rightSiblingId.getCurPage().pid + " = " + recordCount);
 
				 
				 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
					 unpinPage(leafPage.getCurPage(), false);
				 }
 
				 leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
 
				 RID siblingRid = new RID();
				 KeyDataEntry siblingEntry = leafPage.getFirst(siblingRid);
				 while (siblingEntry != null) {
 
					 //System.out.println("distace to query within range search: " + distance);
 
					 nearestNeighbors.add(siblingEntry);
					 
					 siblingEntry = leafPage.getNext(siblingRid);
				 }
 
				 unpinPage(leafPage.getCurPage(), true);
				 rightSiblingId = leafPage.getNextPage(); // Move to the next right sibling
			 }
 
			 if (nearestNeighbors.size() >= number_of_neighbors)
			 {
				 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
					 unpinPage(leafPage.getCurPage(), true);
				 }
				 
				 return nearestNeighbors;
			 }
				 
 
 
			 // ✅ Step 3: If No Right Sibling, Move Up to Parent and Search Adjacent Buckets
			 //System.out.println("🔼 No right sibling. Moving up to parent index...");
 
			 while (!parentNodes.isEmpty()) {
				 PageId parentIndexId = parentNodes.remove(parentNodes.size() - 1); // Get last visited parent
				 LSHFBTIndexPage parentIndexPage = new LSHFBTIndexPage(pinPage(parentIndexId), headerPage.get_keyType());
 
				 RID siblingRid = new RID();
				 KeyDataEntry siblingEntry = parentIndexPage.getFirst(siblingRid);
 
				 while (siblingEntry != null) {
					 PageId siblingPageId = ((IndexData) siblingEntry.data).getData();
 
					 if (!visitedLeaf.contains(siblingPageId)) { // ✅ Check unvisited siblings
						 //System.out.println("🔄 Checking unvisited sibling at Page: " + siblingPageId.pid);
 
						 LSHFBTSortedPage siblingPage = new LSHFBTSortedPage(pinPage(siblingPageId), headerPage.get_keyType());
 
						 if (siblingPage.getType() == NodeType.LEAF) {
							 LSHFBTLeafPage siblingLeafPage = new LSHFBTLeafPage(siblingPage, AttrType.attrVector100D);
							 RID tempRid = new RID();
							 KeyDataEntry tempEntry = siblingLeafPage.getFirst(tempRid);
 
							 while (tempEntry != null) {
								 //System.out.println("✅ Extra Leaf Record: " + tempEntry.key);
								 nearestNeighbors.add(tempEntry);  // ✅ Add to nearest neighbors list
								 visitedLeaf.add(siblingPageId);
								 if (nearestNeighbors.size() >= number_of_neighbors) break;  // Stop when enough records found
								 tempEntry = siblingLeafPage.getNext(tempRid);
							 }
							 unpinPage(siblingPageId);
						 }
					 }
					 siblingEntry = parentIndexPage.getNext(siblingRid);
				 }
				 unpinPage(parentIndexId);
 
				 // ✅ If enough neighbors found, stop searching
				 if (nearestNeighbors.size() >= number_of_neighbors) break;
			 }
 
			 break;  // End search once we’ve exhausted all options
		 }
 
		 // ✅ Return nearest neighbors (or process them)
		 return nearestNeighbors;
 
		 
		 //throw new InsertException(null, "Error finding correct leaf page.");
		 
	 }
 
	 public ArrayList<KeyDataEntry> RangeSearch(String bucketKey, Vector100Dtype query, double range_to_search) 
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
		 
		 //System.out.println("🔍 Target Path: " + bucketKey);
 
		 // ✅ Step 1: Start from the root
		 PageId currentPageId = headerPage.get_rootId();
		 LSHFBTLeafPage leafPage = null;
		 LSHFBTIndexPage indexPage = null;
 
		 ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
		 ArrayList<PageId> parentNodes = new ArrayList<>();
		 ArrayList<PageId> visitedLeaf = new ArrayList<>();
 
		 double highest_val_found = 0.0;
		 
 
		 if (currentPageId.pid == INVALID_PAGE) {
			 System.out.println("⚠️ Tree is empty, creating first leaf page.");
 
			 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
			 PageId newRootPageId = leafPage.getCurPage();
 
			 leafPage.setNextPage(new PageId(INVALID_PAGE));
			 leafPage.setPrevPage(new PageId(INVALID_PAGE));
 
			 System.out.println("✅ Created new ROOT Leaf Node at Page ID: " + newRootPageId.pid);
 
			 unpinPage(newRootPageId, true);
			 updateHeader(newRootPageId);
			 return nearestNeighbors;
		 }
 
		 // ✅ Step 2: Traverse to the last internal node before creating a leaf
		 Page page;
		 String[] keys = bucketKey.split("_");
		 String currentPath = keys[0];
 
		 for (int i = 1; i < keys.length; i++) {
			 currentPath += "_" + keys[i];
			 StringKey pathKey = new StringKey(currentPath);
			 page = pinPage(currentPageId);
			 short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();
			 
			 // System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);
			 // System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
 
			 if (nodeType == NodeType.INDEX) {
				 indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
				 parentNodes.add(currentPageId);
				 PageId nextPageId = indexPage.getPageNoByKey(pathKey);
 
				 // ✅ **We are at the last step of traversal** - determine leaf creation vs. reference
				 if (i == keys.length - 1) {
					 if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
						 // ✅ No leaf exists, create a new one
						 //System.out.println("⚠️ No leaf found at: " + currentPath + " -> Creating new one.");
						 
						 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
						 PageId leafPageId = leafPage.getCurPage();
						 
						 //System.out.println("✅ New Leaf Created at Page ID: " + leafPageId.pid);
						 
						 // ✅ Link the new leaf to the parent index
						 indexPage.insertKey(pathKey, leafPageId);
 
						 //System.out.println("🔎 DEBUG: Verifying Parent Index After Inserting Leaf...");
						 PageId verifyPage = indexPage.getPageNoByKey(pathKey);
						 // if (verifyPage == null || verifyPage.pid == INVALID_PAGE) {
						 // 	System.out.println("❌ ERROR: Parent index did NOT correctly store reference to new leaf!");
						 // } else {
						 // 	System.out.println("✅ Parent index correctly references new leaf at Page ID: " + verifyPage.pid);
						 // }
 
						 // 🚀 Ensure it was inserted properly
						 PageId checkPage = indexPage.getPageNoByKey(pathKey);
						 if (checkPage == null || checkPage.pid == INVALID_PAGE) {
							 //System.out.println("❌ ERROR: Failed to properly link leaf page to index!");
							 throw new InsertException(null, "Leaf was created but not linked properly.");
						 }
 
						 //System.out.println("✅ SUCCESS: Leaf correctly linked at Page ID: " + checkPage.pid);
 
						 // ✅ Reference this for insertion in Step 3
						 currentPageId = leafPageId;
						 unpinPage(currentPageId, true);
						 
					 } else {
						 // ✅ Leaf already exists - reference it for insertion
						 // System.out.println("➡️ Found node at: " + currentPath + " | Current Page ID: " + currentPageId.pid);
						 // System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
						 // System.out.println("✅ Found existing leaf at: " + currentPath);
						 
						 Page nextPage = pinPage(nextPageId);
						 short nextNodeType = new LSHFBTSortedPage(nextPage, headerPage.get_keyType()).getType();
						 
						 if (nextNodeType == NodeType.LEAF) {
							 // ✅ Correctly reference the existing leaf page
							 leafPage = new LSHFBTLeafPage(nextPage, AttrType.attrVector100D);
							 //System.out.println("✅ Confirmed existing leaf at: " + currentPath + " (Page ID: " + nextPageId.pid + ")");
							 unpinPage(nextPageId, true);
 
						 } else {
							 // 🚨 It's actually an index, so we need to create a new leaf instead
							 //System.out.println("⚠️ WARNING: Expected leaf at " + currentPath + ", but found an INDEX instead! Creating a new leaf.");
							 
							 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
							 PageId newLeafPageId = leafPage.getCurPage();
							 
							 // ✅ Link the new leaf to the index page
							 //indexPage.insertKey(pathKey, newLeafPageId);
							 //System.out.println("🔗 Linking new leaf to parent index: " + currentPath + " (Page ID: " + newLeafPageId.pid + ")");
							 indexPage.insertKey(pathKey, newLeafPageId);
 
							 //System.out.println("🔎 DEBUG: Verifying Parent Index After Inserting Leaf...");
							 PageId verifyPage = indexPage.getPageNoByKey(pathKey);
							 // if (verifyPage == null || verifyPage.pid == INVALID_PAGE) {
							 // 	System.out.println("❌ ERROR: Parent index did NOT correctly store reference to new leaf!");
							 // } else {
							 // 	System.out.println("✅ Parent index correctly references new leaf at Page ID: " + verifyPage.pid);
							 // }
												 
							 //System.out.println("✅ SUCCESS: Leaf correctly linked at Page ID: " + verifyPage.pid);
							 currentPageId = newLeafPageId;
							 
							 // ✅ Unpin the newly created leaf so it gets written to disk
							 unpinPage(newLeafPageId, true);
						 }
					 }
 
					 unpinPage(indexPage.getCurPage(), true);
					 break;  // **Exit loop - we found or created the leaf**
				 }
				 unpinPage(currentPageId);  
				 currentPageId = nextPageId;
				 
			 }
		 }
		 //System.out.println("is leaf page null? " + leafPage.getCurPage());
 
		 // ✅ Step 3: Insert the record into the found or newly created leaf
		 while (leafPage != null) {
			 int recordCount = 0;
			 RID countRid = new RID();
			 KeyDataEntry countEntry = leafPage.getFirst(countRid);
 
			 while (countEntry != null) {
				 recordCount++;
				 countEntry = leafPage.getNext(countRid);
			 }
			 //System.out.println("📊 DEBUG: Total Records in Leaf Page " + leafPage.getCurPage().pid + " = " + recordCount);
 
			 // ✅ Process and store leaf entries
			 pinPage(leafPage.getCurPage());
			 RID rid = new RID();
			 KeyDataEntry entry = leafPage.getFirst(rid);
			 while (entry != null) {
				 //System.out.println("✅ Leaf Record: " + entry.key);
				 //((Vector100DKey) entry.key).getKey()
				 double distance = query.computeDistance(query, ((Vector100DKey) entry.key).getKey());
 
				 //System.out.println("distace to query within range search: " + distance);
 
				 if(distance > highest_val_found)
					 highest_val_found = distance;
				 
				 if(distance < highest_val_found)
					 nearestNeighbors.add(entry);  // ✅ Add to nearest neighbors list
				 //if (nearestNeighbors.size() >= number_of_neighbors) break; // Stop when we have enough
				 entry = leafPage.getNext(rid);
			 }
			 unpinPage(leafPage.getCurPage(), true);
 
			 // ✅ Check for right sibling before moving up
			 PageId rightSiblingId = leafPage.getNextPage();
 
			 // if (rightSiblingId.pid != INVALID_PAGE) {
			 // 	//System.out.println("➡️ Moving to existing right sibling: Page " + rightSiblingId.pid);
 
			 // 	if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
			 // 		unpinPage(leafPage.getCurPage(), false);
			 // 	}
 
			 // 	leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
			 // 	continue;
			 // }
 
			 while (rightSiblingId.pid != INVALID_PAGE) { // Keep traversing all right siblings
				 //System.out.println("➡️ Moving to right sibling: Page " + rightSiblingId.pid);
				 
				 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
					 unpinPage(leafPage.getCurPage(), false);
				 }
 
				 leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
 
				 RID siblingRid = new RID();
				 KeyDataEntry siblingEntry = leafPage.getFirst(siblingRid);
				 while (siblingEntry != null) {
 
					 double distance = query.computeDistance(query, ((Vector100DKey) siblingEntry.key).getKey());
					 //System.out.println("distace to query within range search: " + distance);
 
					 if(distance > highest_val_found)
						 highest_val_found = distance;
 
					 if (distance <= range_to_search) {
						 nearestNeighbors.add(siblingEntry);
					 }
					 
					 siblingEntry = leafPage.getNext(siblingRid);
				 }
 
				 unpinPage(leafPage.getCurPage(), true);
				 rightSiblingId = leafPage.getNextPage(); // Move to the next right sibling
			 }
 
			 if (highest_val_found >= range_to_search)
			 {
				 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
					 unpinPage(leafPage.getCurPage(), true);
				 }
				 
				 return nearestNeighbors;
			 }
				 
			 if (highest_val_found >= range_to_search)
				 return nearestNeighbors;
 
			 //System.out.println("Checking parent nodes");
 
			 // ✅ Step 3: If No Right Sibling, Move Up to Parent and Search Adjacent Buckets
			 //System.out.println("🔼 No right sibling. Moving up to parent index...");
 
			 while (!parentNodes.isEmpty()) {
				 PageId parentIndexId = parentNodes.remove(parentNodes.size() - 1); // Get last visited parent
				 LSHFBTIndexPage parentIndexPage = new LSHFBTIndexPage(pinPage(parentIndexId), headerPage.get_keyType());
 
				 RID siblingRid = new RID();
				 KeyDataEntry siblingEntry = parentIndexPage.getFirst(siblingRid);
 
				 while (siblingEntry != null) {
					 PageId siblingPageId = ((IndexData) siblingEntry.data).getData();
 
					 if (!visitedLeaf.contains(siblingPageId)) { // ✅ Check unvisited siblings
						 //System.out.println("🔄 Checking unvisited sibling at Page: " + siblingPageId.pid);
 
						 LSHFBTSortedPage siblingPage = new LSHFBTSortedPage(pinPage(siblingPageId), headerPage.get_keyType());
 
						 if (siblingPage.getType() == NodeType.LEAF) {
							 LSHFBTLeafPage siblingLeafPage = new LSHFBTLeafPage(siblingPage, AttrType.attrVector100D);
							 RID tempRid = new RID();
							 KeyDataEntry tempEntry = siblingLeafPage.getFirst(tempRid);
 
							 while (tempEntry != null) {
								 System.out.println("✅ Extra Leaf Record: " + tempEntry.key);
								 nearestNeighbors.add(tempEntry);  // ✅ Add to nearest neighbors list
								 visitedLeaf.add(siblingPageId);
 
								 double distance = query.computeDistance(query, ((Vector100DKey) tempEntry.key).getKey());
 
								 if(distance > highest_val_found)
									 highest_val_found = distance;
 
								 //if (highest_val_found >= range_to_search) break;  // Stop when enough records found
 
								 tempEntry = siblingLeafPage.getNext(tempRid);
							 }
							 unpinPage(siblingPageId);
						 }
					 }
					 siblingEntry = parentIndexPage.getNext(siblingRid);
				 }
				 unpinPage(parentIndexId);
 
				 //  If enough neighbors found, stop searching
				 if (highest_val_found >= range_to_search) break;
			 }
 
 
 
			 break;  // End search once we’ve exhausted all options
		 }
 
		 //  Return nearest neighbors (or process them)
		 return nearestNeighbors;
 
		 
		 //throw new InsertException(null, "Error finding correct leaf page.");
		 
	 }
 
 
 
 
	 public void insertLeaf(KeyClass key, RID rid, String bucketKey) 
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
		 //System.out.println("🔹 INSERT LEAF START: " + key + " | RID -> Page: " + rid.pageNo.pid + ", Slot: " + rid.slotNo);
		 //System.out.println("🔍 Target Path: " + bucketKey);
 
		 // ✅ Step 1: Start from the root
		 PageId currentPageId = headerPage.get_rootId();
		 LSHFBTLeafPage leafPage = null;
		 LSHFBTIndexPage indexPage = null;
 
		 if (currentPageId.pid == INVALID_PAGE) {
			 //System.out.println("⚠️ Tree is empty, creating first leaf page.");
 
			 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
			 PageId newRootPageId = leafPage.getCurPage();
 
			 leafPage.setNextPage(new PageId(INVALID_PAGE));
			 leafPage.setPrevPage(new PageId(INVALID_PAGE));
 
			 //System.out.println("✅ Created new ROOT Leaf Node at Page ID: " + newRootPageId.pid);
 
			 unpinPage(newRootPageId, true);
			 updateHeader(newRootPageId);
			 return;
		 }
 
		 // ✅ Step 2: Traverse to the last internal node before creating a leaf
		 Page page;
		 String[] keys = bucketKey.split("_");
		 String currentPath = keys[0];
 
		 for (int i = 1; i < keys.length; i++) {
			 currentPath += "_" + keys[i];
			 StringKey pathKey = new StringKey(currentPath);
			 page = pinPage(currentPageId);
			 short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();
			 
			 //System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);
			 //System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
 
			 if (nodeType == NodeType.INDEX) {
				 indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
				 PageId nextPageId = indexPage.getPageNoByKey(pathKey);
 
				 // ✅ **We are at the last step of traversal** - determine leaf creation vs. reference
				 if (i == keys.length - 1) {
					 if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
						 // ✅ No leaf exists, create a new one
						 //System.out.println("⚠️ No leaf found at: " + currentPath + " -> Creating new one.");
						 
						 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
						 PageId leafPageId = leafPage.getCurPage();
						 
						 //System.out.println("✅ New Leaf Created at Page ID: " + leafPageId.pid);
						 
						 // ✅ Link the new leaf to the parent index
						 indexPage.insertKey(pathKey, leafPageId);
 
						 //System.out.println("🔎 DEBUG: Verifying Parent Index After Inserting Leaf...");
						 PageId verifyPage = indexPage.getPageNoByKey(pathKey);
						 // if (verifyPage == null || verifyPage.pid == INVALID_PAGE) {
						 // 	System.out.println("❌ ERROR: Parent index did NOT correctly store reference to new leaf!");
						 // } else {
						 // 	System.out.println("✅ Parent index correctly references new leaf at Page ID: " + verifyPage.pid);
						 // }
 
						 // 🚀 Ensure it was inserted properly
						 PageId checkPage = indexPage.getPageNoByKey(pathKey);
						 if (checkPage == null || checkPage.pid == INVALID_PAGE) {
							 //System.out.println("❌ ERROR: Failed to properly link leaf page to index!");
							 throw new InsertException(null, "Leaf was created but not linked properly.");
						 }
 
						 //System.out.println("✅ SUCCESS: Leaf correctly linked at Page ID: " + checkPage.pid);
 
						 // ✅ Reference this for insertion in Step 3
						 currentPageId = leafPageId;
						 unpinPage(currentPageId, true);
						 
					 } else {
						 // ✅ Leaf already exists - reference it for insertion
						 // System.out.println("➡️ Found node at: " + currentPath + " | Current Page ID: " + currentPageId.pid);
						 // System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
						 // System.out.println("✅ Found existing leaf at: " + currentPath);
						 
						 Page nextPage = pinPage(nextPageId);
						 short nextNodeType = new LSHFBTSortedPage(nextPage, headerPage.get_keyType()).getType();
						 
						 if (nextNodeType == NodeType.LEAF) {
							 // ✅ Correctly reference the existing leaf page
							 leafPage = new LSHFBTLeafPage(nextPage, AttrType.attrVector100D);
							 //System.out.println("✅ Confirmed existing leaf at: " + currentPath + " (Page ID: " + nextPageId.pid + ")");
							 unpinPage(nextPageId, true);
 
						 } else {
							 // 🚨 It's actually an index, so we need to create a new leaf instead
							 //System.out.println("⚠️ WARNING: Expected leaf at " + currentPath + ", but found an INDEX instead! Creating a new leaf.");
							 
							 leafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
							 PageId newLeafPageId = leafPage.getCurPage();
							 
							 // ✅ Link the new leaf to the index page
							 //indexPage.insertKey(pathKey, newLeafPageId);
							 //System.out.println("🔗 Linking new leaf to parent index: " + currentPath + " (Page ID: " + newLeafPageId.pid + ")");
							 indexPage.insertKey(pathKey, newLeafPageId);
 
							 //System.out.println("🔎 DEBUG: Verifying Parent Index After Inserting Leaf...");
							 PageId verifyPage = indexPage.getPageNoByKey(pathKey);
							 // if (verifyPage == null || verifyPage.pid == INVALID_PAGE) {
							 // 	System.out.println("❌ ERROR: Parent index did NOT correctly store reference to new leaf!");
							 // } else {
							 // 	System.out.println("✅ Parent index correctly references new leaf at Page ID: " + verifyPage.pid);
							 // }
												 
							 //System.out.println("✅ SUCCESS: Leaf correctly linked at Page ID: " + verifyPage.pid);
							 currentPageId = newLeafPageId;
							 
							 // ✅ Unpin the newly created leaf so it gets written to disk
							 unpinPage(newLeafPageId, true);
						 }
					 }
 
					 unpinPage(indexPage.getCurPage(), true);
					 break;  // **Exit loop - we found or created the leaf**
				 }
				 unpinPage(currentPageId);  
				 currentPageId = nextPageId;
				 
			 }
		 }
		 //System.out.println("is leaf page null? " + leafPage.getCurPage());
 
		 // ✅ Step 3: Insert the record into the found or newly created leaf
		 while (leafPage != null) {
			 int recordCount = 0;
			 RID countRid = new RID();
			 KeyDataEntry countEntry = leafPage.getFirst(countRid);
			 while (countEntry != null) {
				 recordCount++;
				 countEntry = leafPage.getNext(countRid);
			 }
			 //System.out.println("📊 DEBUG: Total Records in Leaf Page " + leafPage.getCurPage().pid + " = " + recordCount);
 
			 if (recordCount < 38) {
				 //System.out.println("📌 Storing in Leaf: " + key + " under path: " + bucketKey);
 
				 pinPage(leafPage.getCurPage());
				 
				 leafPage.insertRecord(key, rid);
				 //System.out.println("✅ Record Inserted Successfully!");
				 unpinPage(leafPage.getCurPage(), true);
				 return;
			 }
 
			 //System.out.println("⚠️ Leaf Page " + leafPage.getCurPage().pid + " is full! Checking right sibling...");
 
			 PageId rightSiblingId = leafPage.getNextPage();
 
			 if (rightSiblingId.pid != INVALID_PAGE) {
				 //System.out.println("➡️ Moving to existing right sibling: Page " + rightSiblingId.pid);
 
				 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
					 unpinPage(leafPage.getCurPage(), false);
				 }
 
				 leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
 
			 } else {
				 //System.out.println("⚠️ No right sibling found, creating new leaf.");
				 LSHFBTLeafPage newLeaf = new LSHFBTLeafPage(AttrType.attrVector100D);
				 PageId newLeafPageId = newLeaf.getCurPage();
 
				 leafPage.setNextPage(newLeafPageId);
				 newLeaf.setPrevPage(leafPage.getCurPage());
 
				 //System.out.println("✅ Created and linked new Leaf Page: " + newLeafPageId.pid);
 
				 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
					 unpinPage(leafPage.getCurPage(), true);
				 }
				 leafPage = newLeaf;
			 }
			 
			 if (pinCountMap.getOrDefault(leafPage.getCurPage().pid, 0) > 0) {
				 unpinPage(leafPage.getCurPage(), true);
			 }
 
		 }
		 
		 throw new InsertException(null, "Error finding correct leaf page.");
		 
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
		 //System.out.println("Key Length: " + LSHFBT.getKeyLength(key));
 
		 //System.out.println("🔹 INSERT START: " + key);
 
		 if (LSHFBT.getKeyLength(key) > headerPage.get_maxKeySize())
			 throw new KeyTooLongException(null, "");
 
		 if (!(key instanceof StringKey)) {
			 throw new KeyNotMatchException(null, "Only StringKeys allowed for internal nodes.");
		 }
 
		 String bucketKey = ((StringKey) key).getKey();
		 String[] keyParts = bucketKey.split("_");
 
		 //  Step 1: Start from root
		 PageId currentPageId = headerPage.get_rootId();
		 LSHFBTIndexPage currentIndexPage = null;
 
		 if (currentPageId.pid == INVALID_PAGE) {
			 // 🚀 Tree is empty, create first index page
			 //System.out.println("🌱 Tree is empty. Creating first index node.");
			 currentIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
			 PageId newRootPageId = currentIndexPage.getCurPage();
 
			 updateHeader(newRootPageId);
			 headerPage.set_rootId(newRootPageId);
			 currentPageId = newRootPageId;
			 unpinPage(newRootPageId, true);
		 } 
		 else {
			 // ✅ Pin page only when needed
			 Page currentPage = pinPage(currentPageId);
			 LSHFBTSortedPage sortedPage = new LSHFBTSortedPage(currentPage, headerPage.get_keyType());
			 short nodeType = sortedPage.getType();  // ✅ Store the node type before unpinning
 
			 //System.out.println("🔍 ROOT NODE TYPE: " + nodeType);
 
			 if (nodeType == NodeType.BTHEAD) {
				 // 🚨 Convert header into an index node
				 //System.out.println("⚠️ ROOT IS BTHEAD: Converting to index node");
				 currentIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
				 PageId newIndexPageId = currentIndexPage.getCurPage();
				 updateHeader(newIndexPageId);
				 headerPage.set_rootId(newIndexPageId);
				 currentPageId = newIndexPageId;
 
				 // ✅ Ensure the newly created index page is unpinned
				 unpinPage(newIndexPageId, true);
			 }
 
			 // ✅ Unpin the page we pinned earlier
			 unpinPage(currentPageId);
		 }
 
			 // Step 2: Traverse and ensure internal nodes exist
			 String currentPath = keyParts[0];
 
			 for (int i = 1; i < keyParts.length; i++) {
				 currentPath += "_" + keyParts[i];
				 StringKey pathKey = new StringKey(currentPath);
 
				 //System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);
				 //System.out.println("📌 Pinning page: " + currentPageId.pid);
 
				 Page currentPage = pinPage(currentPageId);																	// This isnt being handled correctly
				 if (currentPageId.pid == 10) {
					 //System.out.println("⚠️ DEBUG: Page 10 is being pinned! Need to ensure unpinning.");
				 }
 
				 short nodeType = new LSHFBTSortedPage(currentPage, headerPage.get_keyType()).getType();
 
				 //System.out.println("🔍 Traversing node: " + currentPath + " (Type: " + nodeType + ")");
 
				 //System.out.println("🔍 NODE TYPE at " + currentPath + " is " + nodeType);
				 
 
				 if (nodeType == NodeType.BTHEAD) {
					 //System.out.println("🚨 ERROR: Traversal encountered BTHEAD unexpectedly at: " + currentPath);
					 //System.out.println("🚨 ERROR: Encountered BTHEAD unexpectedly at: " + currentPath);
 
					 // Convert to an actual index node
					 System.out.println("🔄 Converting " + currentPath + " into an INDEX NODE...");
					 LSHFBTIndexPage newIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
					 PageId newIndexPageId = newIndexPage.getCurPage();
 
					 if (currentIndexPage != null) {
						 currentIndexPage.insertKey(new StringKey(currentPath), newIndexPageId);
						 //System.out.println("✅ Inserted index entry for " + currentPath + " at " + newIndexPageId.pid);
					 } else {
						 //System.out.println("⚠️ WARNING: No valid parent found for " + currentPath);
					 }
 
					 currentPageId = newIndexPageId;
					 //unpinPage(currentPageId);
					 
					 unpinPage(newIndexPageId, true);
				 }
 
				 if (nodeType == NodeType.LEAF) {
					 //System.out.println("⚠️ Warning: Expected an internal node, but found a LEAF at: " + currentPath);
 
					 //System.out.println("✅ Found leaf node at: " + currentPath);
 
					 // 🚨 Check if an internal node already exists
					 PageId existingPageId = currentIndexPage.getPageNoByKey(pathKey);
					 if (existingPageId != null && existingPageId.pid != INVALID_PAGE) {
						 //System.out.println("✅ Skipping redundant creation of index node at: " + currentPath);
						 //System.out.println("🔄 Using existing index node for: " + currentPath + " at Page ID " + existingPageId.pid);
						 currentPageId = existingPageId;  // Move forward without creating a new node
						 if (pinCountMap.getOrDefault(currentPageId.pid, 0) > 0) {
							 unpinPage(currentPageId);
						 }
						 //unpinPage(currentPageId);
						 continue;
					 }
 
					 // Convert the leaf node into an internal index node
					 //System.out.println("🔄 Converting LEAF to INDEX at: " + currentPath);
					 LSHFBTIndexPage newIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
					 PageId newIndexPageId = newIndexPage.getCurPage();
 
					 // Update the parent to store reference to this as an index node
					 if (currentIndexPage != null) {
						 currentIndexPage.insertKey(pathKey, newIndexPageId);
						 //System.out.println("✅ Inserted index entry for " + currentPath + " at " + newIndexPageId.pid);
					 } else {
						 //System.out.println("⚠️ WARNING: No valid parent found for " + currentPath);
					 }
 
					 // Unpin the newly created index page to persist it
					 unpinPage(newIndexPageId, true);
 
					 // Update traversal to point to the new index page
					 currentPageId = newIndexPageId;
					 unpinPage(currentPageId);
					 // Continue traversal now that the new index node exists
					 continue;
				 }
 
				 if (nodeType == NodeType.INDEX) {
					 //System.out.println("🛠️ Found INTERNAL NODE: " + currentPath);
					 currentIndexPage = new LSHFBTIndexPage(currentPage, headerPage.get_keyType());
					 PageId nextPageId = currentIndexPage.getPageNoByKey(pathKey);
 
					 if (nextPageId == null || nextPageId.pid == INVALID_PAGE || nextPageId.pid == headerPage.get_rootId().pid) {
						 if (i == keyParts.length - 1) {  
							 // ✅ We are at the LAST level → Create a LEAF page instead of an INDEX page
							 //System.out.println("✅ Reached last level of index structure at: " + currentPath);
							 unpinPage(currentPageId);	
								continue;
						 } else {  
							 // ✅ We are NOT at the last level → Create an INDEX page
							 //System.out.println("⚠️ No child node exists for " + currentPath + ", creating an INDEX page.");
							 LSHFBTIndexPage newIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
							 nextPageId = newIndexPage.getCurPage();
							 //System.out.println("✅ Inserted INDEX for " + currentPath + " at " + nextPageId.pid);
						 }
 
						 // ✅ Ensure parent correctly stores reference to the new page (leaf or index)
						 currentIndexPage.insertKey(pathKey, nextPageId);
						 //System.out.println("✅ Parent now references " + currentPath + " at Page ID: " + nextPageId.pid);
 
						 if (currentIndexPage.getPrevPage().pid == INVALID_PAGE) {
							 //System.out.println("🛠️ FIX: Setting PrevPage for Root Index Node to " + nextPageId.pid);
							 currentIndexPage.setPrevPage(nextPageId);
						 }
 
						 // ✅ Explicitly unpin the new page to persist it
						 unpinPage(nextPageId, true);
					 }
					 if (nextPageId != null && nextPageId.pid != INVALID_PAGE) {
						 unpinPage(currentPageId);
					 }
					 currentPageId = nextPageId;
				 }
				 else {
					 //System.out.println("❌ ERROR: Unexpected node type at " + currentPath);
					 unpinPage(currentPageId);
					 return;
				 }
 
				 if (pinCountMap.getOrDefault(currentPageId.pid, 0) > 0) {
					 unpinPage(currentPageId);
				 }
				 //unpinPage(currentPageId);
			 }
		 //unpinPage(currentPageId, true);
		 //System.out.println("🔹 Inserted Internal Node: " + bucketKey);
 
 
		 
		 
		 // TWO CASES:
		 // 1. headerPage.root == INVALID_PAGE:
		 //    - the tree is empty and we have to create a new first page;
		 //    this page will be a leaf page
		 // 2. headerPage.root != INVALID_PAGE:
		 //    - we call _insert() to insert the pair (key, rid)
		 
		 
		 // if ( trace != null )
		 // {
		 // trace.writeBytes( "INSERT " + rid.pageNo + " "
		 // 			+ rid.slotNo + " " + key + lineSep);
		 // trace.writeBytes( "DO" + lineSep);
		 // trace.flush();
		 // }
		 
		 
		 // if (headerPage.get_rootId().pid == INVALID_PAGE) {
		 // PageId newRootPageId;
		 // LSHFBTLeafPage newRootPage;
		 // RID dummyrid;
		 
		 // newRootPage=new LSHFBTLeafPage( headerPage.get_keyType());
		 // newRootPageId=newRootPage.getCurPage();
		 
		 
		 // if ( trace != null )
		 // {
		 // 	trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
		 // 	trace.flush();
		 // }
		 
		 
		 
		 // newRootPage.setNextPage(new PageId(INVALID_PAGE));
		 // newRootPage.setPrevPage(new PageId(INVALID_PAGE));
		 
		 
		 // // ASSERTIONS:
		 // // - newRootPage, newRootPageId valid and pinned
		 
		 // newRootPage.insertRecord(key, rid); 
		 
		 // if ( trace!=null )
		 // {
		 // 		trace.writeBytes("PUTIN node " + newRootPageId+lineSep);
		 // 		trace.flush();
		 // }
		 
		 // unpinPage(newRootPageId, true); /* = DIRTY */
		 // updateHeader(newRootPageId);
		 
		 // if ( trace!=null )
		 // {
		 // 		trace.writeBytes("DONE" + lineSep);
		 // 		trace.flush();
		 // }
			 
		 
		 // 	return;
		 // }
		 
		 // // ASSERTIONS:
		 // // - headerPageId, headerPage valid and pinned
		 // // - headerPage.root holds the pageId of the root of the B-tree
		 // // - none of the pages of the tree is pinned yet
		 
		 
		 // if ( trace != null )
		 // {
		 // trace.writeBytes( "SEARCH" + lineSep);
		 // trace.flush();
		 // }
		 
		 
		 // newRootEntry= _insert(key, rid, headerPage.get_rootId());
		 
		 // // TWO CASES:
		 // // - newRootEntry != null: a leaf split propagated up to the root
		 // //                            and the root split: the new pageNo is in
		 // //                            newChildEntry.data.pageNo 
		 // // - newRootEntry == null: no new root was created;
		 // //                            information on headerpage is still valid
		 
		 // // ASSERTIONS:
		 // // - no page pinned
		 
		 // if (newRootEntry != null)
		 // {
		 // LSHFBTIndexPage newRootPage;
		 // PageId      newRootPageId;
		 // Object      newEntryKey;
		 
		 // // the information about the pair <key, PageId> is
		 // // packed in newRootEntry: extract it
		 
		 // newRootPage = new LSHFBTIndexPage(headerPage.get_keyType());
		 // newRootPageId=newRootPage.getCurPage();
		 
		 // // ASSERTIONS:
		 // // - newRootPage, newRootPageId valid and pinned
		 // // - newEntryKey, newEntryPage contain the data for the new entry
		 // //     which was given up from the level down in the recursion
		 
		 
		 // if ( trace != null )
		 // 	{
		 // 	trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
		 // 	trace.flush();
		 // 	}
		 
		 
		 // newRootPage.insertKey( newRootEntry.key, 
		 // 			((IndexData)newRootEntry.data).getData() );
		 
		 
		 // // the old root split and is now the left child of the new root
		 // newRootPage.setPrevPage(headerPage.get_rootId());
		 
		 // unpinPage(newRootPageId, true /* = DIRTY */);
		 
		 // updateHeader(newRootPageId);
		 
		 // }
		 
		 
		 // if ( trace !=null )
		 // {
		 // trace.writeBytes("DONE"+lineSep);
		 // trace.flush();
		 // }
		 
		 
		 // return;
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
 
 
			 // System.out.println(" DEBUG: Processing Key: " + key);
			 // System.out.println(" DEBUG: Current Node Type: " + currentPage.getType());
 
			 if (currentPage.getType() == NodeType.LEAF) {
				 //System.out.println(" ERROR: Key " + key + " mistakenly reaching a LEAF node! ABORTING.");
				 unpinPage(currentPageId);
				 return null;
			 }
 
			 
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
			 
			 if(currentPage.getType() == NodeType.INDEX) 
			 {
				 LSHFBTIndexPage  currentIndexPage=new LSHFBTIndexPage(page,headerPage.get_keyType());
				 PageId currentIndexPageId = currentPageId;
				 PageId nextPageId;
					 //System.out.println(" DEBUG: Processing Key: " + key);
				 //System.out.println(" DEBUG: Current Node Type: " + currentPage.getType());
 
				 if (key instanceof StringKey) {
					 try {
						 //  Retrieve the next page ID
						 nextPageId = currentIndexPage.getPageNoByKey(key);
 
						 // If the key does not exist, create a new child page
						 if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
							 nextPageId = new PageId();
							 LSHFBTIndexPage newIndexPage = new LSHFBTIndexPage(nextPageId, headerPage.get_keyType());
							 unpinPage(nextPageId, true);  //  Ensure this always unpins
						 }
 
						 //  Now insert the key with the correct child page reference
						 currentIndexPage.insertKey(key, nextPageId);
					 } finally {
						 unpinPage(currentIndexPageId, true);  //  Ensure the parent is unpinned
					 }
					 return null;  // Stop recursing for internal nodes
				 }
 
				 nextPageId=currentIndexPage.getPageNoByKey(key);
				 
				 // now unpin the page, recurse and then pin it again
				 unpinPage(currentIndexPageId);
				 
				 return _insert(key, rid, nextPageId);
			 }
 
			 unpinPage(currentPageId);
			 return null;
 
		 }
 
 
 
		 public void traverseAllBuckets() 
		 throws IOException, PinPageException, UnpinPageException, ConstructPageException, IteratorException {
 
		 System.out.println("🔍 Starting Full LSHF Traversal...");
 
		 PageId rootPageId = headerPage.get_rootId();
		 if (rootPageId.pid == INVALID_PAGE) {
			 System.out.println("⚠️ No root found, empty structure.");
			 return;
		 }
 
		 Queue<PageId> queue = new LinkedList<>();
		 queue.add(rootPageId);
 
		 while (!queue.isEmpty()) {
			 PageId currentPageId = queue.poll();
			 Page currentPage = pinPage(currentPageId);
			 LSHFBTSortedPage sortedPage = new LSHFBTSortedPage(currentPage, headerPage.get_keyType());
 
			 System.out.println("📌 Visiting Page: " + currentPageId.pid);
 
			 if (sortedPage.getType() == NodeType.LEAF) {
				 // ✅ Process the leaf node (bucket)
				 LSHFBTLeafPage leafPage = new LSHFBTLeafPage(currentPage, headerPage.get_keyType());
				 printLeafEntries(leafPage);
			 } else if (sortedPage.getType() == NodeType.INDEX) {
				 // ✅ Process index nodes (add children to queue for BFS)
				 LSHFBTIndexPage indexPage = new LSHFBTIndexPage(currentPage, headerPage.get_keyType());
				 RID rid = new RID();
				 KeyDataEntry entry = indexPage.getFirst(rid);
				 
				 while (entry != null) {
					 PageId childPageId = ((IndexData) entry.data).getData();
					 queue.add(childPageId); // Add all child nodes to queue
					 entry = indexPage.getNext(rid);
				 }
			 }
 
			 unpinPage(currentPageId);
		 }
 
		 System.out.println("✅ Full Traversal Completed.");
	 }
 
	 private void printLeafEntries(LSHFBTLeafPage leafPage) throws IOException, IteratorException {
		 System.out.println("📄 Leaf Page " + leafPage.getCurPage().pid + " Entries:");
		 
		 RID rid = new RID();
		 KeyDataEntry entry = leafPage.getFirst(rid);
 
		 while (entry != null) {
			 System.out.println("   🔹 Key: " + entry.key + " -> Data: " + entry.data);
			 entry = leafPage.getNext(rid);
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
		UnpinPageException,
		IndexSearchException
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
 
	   System.out.println("");
	   System.out.println("");
	 
	   System.out.println("🔍 [findRunStart] Root ID Retrieved: " + pageno.pid);
 
		 if (pageno == null || pageno.pid == INVALID_PAGE) {
			 System.out.println("❌ ERROR: Root page is INVALID or NULL.");
			 return null;  // Early exit to prevent further errors
		 }
	   
	   if (pageno.pid == INVALID_PAGE){        // no pages in the BTREE
		 pageLeaf = null;                // should be handled by 
		 // startrid =INVALID_PAGEID ;             // the caller
		 return pageLeaf;
	   }
	   System.out.println("📌 [findRunStart] Attempting to Pin Root Page ID: " + pageno.pid);
	   page= pinPage(pageno);
	   sortPage=new LSHFBTSortedPage(page, headerPage.get_keyType());
	   System.out.println("✅ [findRunStart] Successfully Pinned Root Page ID: " + pageno.pid);
 
	   System.out.println("🔍 [findRunStart] Root Page Type: " + sortPage.getType());
	   
	   if ( trace!=null ) {
		 trace.writeBytes("VISIT node " + pageno + lineSep);
		 trace.flush();
	   }
	   
	   
	   // ASSERTION
	   // - pageno and sortPage is the root of the btree
	   // - pageno and sortPage valid and pinned
	   
	   while (sortPage.getType() == NodeType.INDEX) {
		 System.out.println("➡️ Traversing INDEX Node: " + pageno.pid);
 
		 pageIndex=new LSHFBTIndexPage(page, headerPage.get_keyType()); 
		 prevpageno = pageIndex.getPrevPage();
		 System.out.println("🔍 [findRunStart] Initial PrevPage ID: " + prevpageno.pid);
 
		 System.out.println("🔎 [findRunStart] Checking all keys in INDEX Node: " + pageno.pid);
		 RID checkRid = new RID();
		 KeyDataEntry checkEntry = pageIndex.getFirst(checkRid);
		 while (checkEntry != null) {
			 System.out.println("   🔑 Key: " + checkEntry.key + " -> Child Page: " + ((IndexData) checkEntry.data).getData().pid);
			 checkEntry = pageIndex.getNext(checkRid);
		 }
 
		 // Get the correct child page reference
		 KeyDataEntry firstEntry = pageIndex.getFirst(startrid);
		 if (firstEntry != null) {
			 prevpageno = ((IndexData) firstEntry.data).getData();  // Get actual child page
		 }
 
		 // 🚀 **NEW DEBUG: Confirm child page before accessing**
		 System.out.println("📌 DEBUG: Attempting to Pin Child Page ID: " + prevpageno.pid);
		 Page testPage = pinPage(prevpageno);
		 if (testPage == null) {
			 System.out.println("❌ ERROR: Failed to fetch page " + prevpageno.pid + " from buffer pool.");
		 } else {
			 System.out.println("✅ Successfully fetched page: " + prevpageno.pid);
		 }
 
		 System.out.println("🔎 [findRunStart] Checking all keys in INDEX Node: " + pageno.pid);
		 checkRid = new RID();
		 checkEntry = pageIndex.getFirst(checkRid);
		 while (checkEntry != null) {
			 System.out.println("   🔑 Key: " + checkEntry.key + " -> Child Page: " + ((IndexData) checkEntry.data).getData().pid);
			 checkEntry = pageIndex.getNext(checkRid);
		 }
 
		 // 🚨 **NEW DEBUG: Check if First Entry Exists Before Accessing Key**
		 firstEntry = pageIndex.getFirst(startrid);
		 if (firstEntry == null) {
			 System.out.println("❌ ERROR: This index node has NO valid key entries!");
		 } else {
			 PageId testChild = pageIndex.getPageNoByKey(firstEntry.key);
			 if (testChild == null || testChild.pid == INVALID_PAGE) {
				 System.out.println("❌ ERROR: This index has NO valid child page! Checking last valid key...");
				 RID lastRid = new RID();
				 KeyDataEntry lastEntry = pageIndex.getFirst(lastRid);
 
				 // Iterate through the index node to get the last valid key
				 KeyDataEntry tempEntry = lastEntry;
				 while (tempEntry != null) {
					 lastEntry = tempEntry;
					 tempEntry = pageIndex.getNext(lastRid);
				 }
 
				 // Check if we found a valid last entry
				 if (lastEntry == null) {
					 System.out.println("❌ Still No Valid Entry! Aborting.");
					 return null;
				 } else {
					 testChild = ((IndexData) lastEntry.data).getData();
					 System.out.println("✅ Using Last Key Instead: " + testChild.pid);
				 }
 
				 if (testChild == null || testChild.pid == INVALID_PAGE) {
					 System.out.println("❌ Still No Valid Entry! Aborting.");
					 return null;
				 } else {
					 System.out.println("✅ Using Last Key Instead: " + testChild.pid);
				 }
			 } else {
				 System.out.println("✅ Expected Child Page ID: " + testChild.pid);
			 }
		 }
 
 
 
		 curEntry= pageIndex.getFirst(startrid);
		 while ( curEntry!=null && lo_key != null 
			 && LSHFBT.keyCompare(curEntry.key, lo_key) < 0) {
		 
			 prevpageno = ((IndexData)curEntry.data).getData();
			 curEntry=pageIndex.getNext(startrid);
		 }
 
		 // 🚀 Debug: Verify Parent Index Key-Child Relationships
		 System.out.println("🔎 DEBUG: Validating Parent Index Keys Before Moving to Child: " + pageIndex.getCurPage().pid);
		 RID debugRid = new RID();
		 KeyDataEntry debugEntry = pageIndex.getFirst(debugRid);
		 while (debugEntry != null) {
			 System.out.println("   🔑 Stored Key: " + debugEntry.key + " -> Child Page: " + ((IndexData) debugEntry.data).getData().pid);
			 debugEntry = pageIndex.getNext(debugRid);
		 }
 
		 // 🚨 **NEW FINAL CHECK: If prevpageno is STILL INVALID, prevent crash**
		 if (prevpageno == null || prevpageno.pid == INVALID_PAGE) {
			 System.out.println("❌ ERROR: Traversal resulted in an INVALID child page. Checking for last known leaf...");
			 KeyDataEntry lastValidEntry = pageIndex.getFirst(startrid);
			 KeyDataEntry tempEntry;
 
			 while ((tempEntry = pageIndex.getNext(startrid)) != null) {
				 lastValidEntry = tempEntry;  // Keep updating until we reach the last key
			 }
			 if (lastValidEntry != null) {
				 prevpageno = ((IndexData) lastValidEntry.data).getData();
				 System.out.println("✅ Found a valid fallback child page: " + prevpageno.pid);
			 } else {
				 System.out.println("❌ ERROR: No fallback leaf found. Returning NULL.");
				 return null;
			 }
		 }
 
		 System.out.println("🔄 [findRunStart] Moving to Child Page ID: " + prevpageno.pid);
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
	   
	   pageLeaf = new LSHFBTLeafPage(page, AttrType.attrVector100D);
	   
 
	   System.out.println("🔍 DEBUG: Checking Leaf Page " + pageno.pid + " for Entries...");
		 RID testRid = new RID();
		 KeyDataEntry testEntry = pageLeaf.getFirst(testRid);
 
		 if (testEntry == null) {
			 System.out.println("⚠️ WARNING: Leaf Page " + pageno.pid + " is EMPTY!");
		 } else {
			 System.out.println("✅ Found Entry in Leaf Page: " + testEntry.key + " -> " + testEntry.data);
		 }
	   curEntry=pageLeaf.getFirst(startrid);
	   while (curEntry==null) 
	   {
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
		   unpinPage(pageno);
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
	   unpinPage(pageno);
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
		UnpinPageException,
		IndexSearchException
		
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
 
 
 