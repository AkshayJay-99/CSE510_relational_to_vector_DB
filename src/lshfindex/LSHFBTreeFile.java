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
 import java.util.Arrays;
 
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
		 
		 SystemDefs.JavabaseBM.unpinPage(pageno, true /* = not DIRTY */);    
		 
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

	public ArrayList<KeyDataEntry> SingleFileRead(String bucketKey, boolean fullKeyUsed) 
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
		PageId currentPageId = headerPage.get_rootId();
		LSHFBTLeafPage leafPage = null;
		LSHFBTIndexPage indexPage = null;

		ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
		ArrayList<PageId> parentNodes = new ArrayList<>();
		ArrayList<PageId> visitedLeaf = new ArrayList<>();

		if (currentPageId.pid == INVALID_PAGE) {
			System.out.println("⚠️ Tree is empty.");
			return nearestNeighbors;
		}

		Page page;
		String[] keys = bucketKey.split("_");
		String currentPath = keys[0];

		for (int i = 1; i < keys.length; i++) {
			currentPath += "_" + keys[i];
			StringKey pathKey = new StringKey(currentPath);

			page = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();

			//System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);

			if (nodeType != NodeType.INDEX) {
				throw new NodeNotMatchException(null, "Expected INDEX node at " + currentPath);
			}

			indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
			parentNodes.add(currentPageId);


			//System.out.println("bucketkey: " + bucketKey+ " vs currentKey: "+ currentPath);

			PageId nextPageId = indexPage.getPageNoByKey(pathKey);

			if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
				unpinPage(currentPageId);
				System.out.println("⚠️ No child found at: " + currentPath);
				return nearestNeighbors;
			}

			if (i == keys.length - 1) {
				// 🌟 FULL PREFIX MATCH: Look up full bucket key -> should find leaf
				StringKey fullKey = new StringKey(currentPath);	//new StringKey(bucketKey);
				PageId leafPageId = indexPage.getPageNoByKey(fullKey);

				if (leafPageId == null || leafPageId.pid == INVALID_PAGE) {
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ No leaf linked to: " + currentPath);
					return nearestNeighbors;
				}

				// 🛡️ Pin the page and check if it's a real LEAF
				Page leafCandidatePage = pinPage(leafPageId);
				short leafNodeType = new LSHFBTSortedPage(leafCandidatePage, headerPage.get_keyType()).getType();

				if (leafNodeType != NodeType.LEAF) {
					// 🚨 Wrong node type: unpin and exit
					unpinPage(leafPageId);
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ Page under " + currentPath + " is not a LEAF (type: " + leafNodeType + ")");
					return nearestNeighbors;
				}

				// ✅ Now safely treat it as a leaf
				leafPage = new LSHFBTLeafPage(leafCandidatePage, AttrType.attrVector100D);

				unpinPage(indexPage.getCurPage(), false); // Done with index
				break; // Exit traversal
			}

			unpinPage(currentPageId);
			currentPageId = nextPageId;
		}

		//System.out.println("We followed the current path: " + currentPath);
		searchedNodes.add(currentPath);



		if (leafPage == null) {
			System.out.println("⚠️ Leaf page not found.");
			return nearestNeighbors;
		}

		// ✅ Now scan from the correct leaf page
		while (leafPage != null) {
			RID rid = new RID();
			KeyDataEntry entry = leafPage.getFirst(rid);

			while (entry != null) {
				nearestNeighbors.add(entry);
				// if (nearestNeighbors.size() >= number_of_neighbors) {
				// 	break;
				// }
				entry = leafPage.getNext(rid);
			}

			// if (nearestNeighbors.size() >= number_of_neighbors) {
			// 	break;
			// }

			PageId rightSiblingId = leafPage.getNextPage();
			if (rightSiblingId.pid == INVALID_PAGE) {
				break;
			}

			unpinPage(leafPage.getCurPage(), false);
			leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
		}

		if (leafPage != null) {
			unpinPage(leafPage.getCurPage(), false);
		}

		return nearestNeighbors;
	}


	public ArrayList<KeyDataEntry> DeleteFile(String bucketKey, Vector100Dtype key) 
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
           IOException,
		   FreePageException
	{
		PageId currentPageId = headerPage.get_rootId();
		LSHFBTLeafPage leafPage = null;
		LSHFBTIndexPage indexPage = null;

		ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
		ArrayList<PageId> parentNodes = new ArrayList<>();
		ArrayList<PageId> visitedLeaf = new ArrayList<>();

		if (currentPageId.pid == INVALID_PAGE) {
			System.out.println("⚠️ Tree is empty.");
			return nearestNeighbors;
		}

		Page page;
		String[] keys = bucketKey.split("_");
		String currentPath = keys[0];

		for (int i = 1; i < keys.length; i++) {
			currentPath += "_" + keys[i];
			StringKey pathKey = new StringKey(currentPath);

			page = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();

			//System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);

			if (nodeType != NodeType.INDEX) {
				throw new NodeNotMatchException(null, "Expected INDEX node at " + currentPath);
			}

			indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
			parentNodes.add(currentPageId);


			//System.out.println("bucketkey: " + bucketKey+ " vs currentKey: "+ currentPath);

			PageId nextPageId = indexPage.getPageNoByKey(pathKey);

			if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
				unpinPage(currentPageId);
				System.out.println("⚠️ No child found at: " + currentPath);
				return nearestNeighbors;
			}

			if (i == keys.length - 1) {
				// 🌟 FULL PREFIX MATCH: Look up full bucket key -> should find leaf
				StringKey fullKey = new StringKey(currentPath);	//new StringKey(bucketKey);
				PageId leafPageId = indexPage.getPageNoByKey(fullKey);

				if (leafPageId == null || leafPageId.pid == INVALID_PAGE) {
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ No leaf linked to: " + currentPath);
					return nearestNeighbors;
				}

				// 🛡️ Pin the page and check if it's a real LEAF
				Page leafCandidatePage = pinPage(leafPageId);
				short leafNodeType = new LSHFBTSortedPage(leafCandidatePage, headerPage.get_keyType()).getType();

				if (leafNodeType != NodeType.LEAF) {
					// 🚨 Wrong node type: unpin and exit
					unpinPage(leafPageId);
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ Page under " + currentPath + " is not a LEAF (type: " + leafNodeType + ")");
					return nearestNeighbors;
				}

				// ✅ Now safely treat it as a leaf
				leafPage = new LSHFBTLeafPage(leafCandidatePage, AttrType.attrVector100D);

				unpinPage(indexPage.getCurPage(), false); // Done with index
				break; // Exit traversal
			}

			unpinPage(currentPageId);
			currentPageId = nextPageId;
		}

		//System.out.println("We followed the current path: " + currentPath);
		//searchedNodes.add(currentPath);



		if (leafPage == null) {
			System.out.println("⚠️ Leaf page not found.");
			return nearestNeighbors;
		}

		// ✅ Now scan from the correct leaf page
		while (leafPage != null) {


			RID rid = new RID();
			KeyDataEntry entry = leafPage.getFirst(rid);

			//leafPage.delEntry(entry);

			while (entry != null) {
				//nearestNeighbors.add(entry);
				// if (nearestNeighbors.size() >= number_of_neighbors) {
				// 	break;
				// }
				
				Vector100Dtype contender = ((Vector100DKey) entry.key).getKey();

					
				if(Arrays.equals(key.getValues(), contender.getValues()))
				{
					//System.out.println(leafPage.available_space());
					//System.out.println("We found a match: ");
					leafPage.delEntry(entry);

					//System.out.println(leafPage.available_space());
				}
				

				// if(contender.getValues() == key.getValues())
				// 	System.out.println("we found a match!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

				entry = leafPage.getNext(rid);
			}



			//leafPage.deleteSortedRecord(rid);

			// if (nearestNeighbors.size() >= number_of_neighbors) {
			// 	break;
			// }

			PageId rightSiblingId = leafPage.getNextPage();
			if (rightSiblingId.pid == INVALID_PAGE) {
				break;
			}

			unpinPage(leafPage.getCurPage(), true);
			leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
		}

		//freePage(leafPage.getCurPage());

		if (leafPage != null) {
			unpinPage(leafPage.getCurPage(), true);
			
		}

		return nearestNeighbors;
	}
	


	public ArrayList<String> searchedNodes = new ArrayList();

	public String lastVisited = "";
 
	public ArrayList<KeyDataEntry> NNSearch(String bucketKey, int number_of_neighbors, boolean fullKeyUsed) 
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
		PageId currentPageId = headerPage.get_rootId();
		LSHFBTLeafPage leafPage = null;
		LSHFBTIndexPage indexPage = null;

		ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
		ArrayList<PageId> parentNodes = new ArrayList<>();
		ArrayList<PageId> visitedLeaf = new ArrayList<>();

		if (currentPageId.pid == INVALID_PAGE) {
			System.out.println("⚠️ Tree is empty.");
			return nearestNeighbors;
		}

		Page page;
		String[] keys = bucketKey.split("_");
		String currentPath = keys[0];

		for (int i = 1; i < keys.length; i++) {
			currentPath += "_" + keys[i];
			StringKey pathKey = new StringKey(currentPath);

			page = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();

			//System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);

			if (nodeType != NodeType.INDEX) {
				throw new NodeNotMatchException(null, "Expected INDEX node at " + currentPath);
			}

			indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
			parentNodes.add(currentPageId);

			RID childRid = new RID();
			KeyDataEntry childEntry = indexPage.getFirst(childRid);

			boolean foundNewPath = false;

			while (childEntry != null) {

				//System.out.println("       Child key: " + childEntry.key.toString() + " -> Child PageId: " + ((IndexData)childEntry.data).getData().pid);

				if(keys[i].equals("*") && !searchedNodes.contains(childEntry.key.toString()))
				{
					//System.out.println("We found a key to replace: " + currentPath + " with: " + childEntry.key.toString());
					//System.out.println("we are going to use: " + childEntry.key.toString());
					String newPath = childEntry.key.toString();
					pathKey = new StringKey(newPath);
					//System.out.println("pathkey: "+pathKey.getKey());
					currentPath = childEntry.key.toString();
					foundNewPath = true;
				}

				if(!keys[i].equals("*"))
				{
					foundNewPath = true;
				}


				childEntry = indexPage.getNext(childRid);
			}
			// **** End print of children.

			if(foundNewPath == false && fullKeyUsed == false)
			{
				//System.out.println("no more potential paths at: " + currentPath);
				String[] explored = currentPath.split("_\\*");
				//System.out.println(explored[0]);

				searchedNodes.add(explored[0]);
				lastVisited = currentPath;
				return nearestNeighbors;
			}
				

			//System.out.println("bucketkey: " + bucketKey+ " vs currentKey: "+ currentPath);

			PageId nextPageId = indexPage.getPageNoByKey(pathKey);

			if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
				unpinPage(currentPageId);
				System.out.println("⚠️ No child found at: " + currentPath);
				return nearestNeighbors;
			}

			if (i == keys.length - 1) {
				// 🌟 FULL PREFIX MATCH: Look up full bucket key -> should find leaf
				StringKey fullKey = new StringKey(currentPath);	//new StringKey(bucketKey);
				PageId leafPageId = indexPage.getPageNoByKey(fullKey);

				if (leafPageId == null || leafPageId.pid == INVALID_PAGE) {
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ No leaf linked to: " + currentPath);
					return nearestNeighbors;
				}

				// 🛡️ Pin the page and check if it's a real LEAF
				Page leafCandidatePage = pinPage(leafPageId);
				short leafNodeType = new LSHFBTSortedPage(leafCandidatePage, headerPage.get_keyType()).getType();

				if (leafNodeType != NodeType.LEAF) {
					// 🚨 Wrong node type: unpin and exit
					unpinPage(leafPageId);
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ Page under " + currentPath + " is not a LEAF (type: " + leafNodeType + ")");
					return nearestNeighbors;
				}

				// ✅ Now safely treat it as a leaf
				leafPage = new LSHFBTLeafPage(leafCandidatePage, AttrType.attrVector100D);

				unpinPage(indexPage.getCurPage(), false); // Done with index
				break; // Exit traversal
			}

			unpinPage(currentPageId);
			currentPageId = nextPageId;
		}

		//System.out.println("We followed the current path: " + currentPath);
		searchedNodes.add(currentPath);



		if (leafPage == null) {
			System.out.println("⚠️ Leaf page not found.");
			return nearestNeighbors;
		}

		// ✅ Now scan from the correct leaf page
		while (leafPage != null) {
			RID rid = new RID();
			KeyDataEntry entry = leafPage.getFirst(rid);

			while (entry != null) {
				nearestNeighbors.add(entry);
				// if (nearestNeighbors.size() >= number_of_neighbors) {
				// 	break;
				// }
				entry = leafPage.getNext(rid);
			}

			// if (nearestNeighbors.size() >= number_of_neighbors) {
			// 	break;
			// }

			PageId rightSiblingId = leafPage.getNextPage();
			if (rightSiblingId.pid == INVALID_PAGE) {
				break;
			}

			unpinPage(leafPage.getCurPage(), false);
			leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
		}

		if (leafPage != null) {
			unpinPage(leafPage.getCurPage(), false);
		}

		return nearestNeighbors;
	}

	public double highestDistanceFound = 0;

	public ArrayList<KeyDataEntry> RangeSearch(String bucketKey, double range, boolean fullKeyUsed, Vector100Dtype query) 
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
		PageId currentPageId = headerPage.get_rootId();
		LSHFBTLeafPage leafPage = null;
		LSHFBTIndexPage indexPage = null;

		ArrayList<KeyDataEntry> nearestNeighbors = new ArrayList<>();
		ArrayList<PageId> parentNodes = new ArrayList<>();
		ArrayList<PageId> visitedLeaf = new ArrayList<>();

		if (currentPageId.pid == INVALID_PAGE) {
			System.out.println("⚠️ Tree is empty.");
			return nearestNeighbors;
		}

		Page page;
		String[] keys = bucketKey.split("_");
		String currentPath = keys[0];

		for (int i = 1; i < keys.length; i++) {
			currentPath += "_" + keys[i];
			StringKey pathKey = new StringKey(currentPath);

			page = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();

			//System.out.println("➡️ TRAVERSING: " + currentPath + " | Current Page ID: " + currentPageId.pid);

			if (nodeType != NodeType.INDEX) {
				throw new NodeNotMatchException(null, "Expected INDEX node at " + currentPath);
			}

			indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
			parentNodes.add(currentPageId);

			RID childRid = new RID();
			KeyDataEntry childEntry = indexPage.getFirst(childRid);

			boolean foundNewPath = false;

			while (childEntry != null) {

				//System.out.println("       Child key: " + childEntry.key.toString() + " -> Child PageId: " + ((IndexData)childEntry.data).getData().pid);

				if(keys[i].equals("*") && !searchedNodes.contains(childEntry.key.toString()))
				{
					//System.out.println("We found a key to replace: " + currentPath + " with: " + childEntry.key.toString());
					//System.out.println("we are going to use: " + childEntry.key.toString());
					String newPath = childEntry.key.toString();
					pathKey = new StringKey(newPath);
					//System.out.println("pathkey: "+pathKey.getKey());
					currentPath = childEntry.key.toString();
					foundNewPath = true;
				}

				if(!keys[i].equals("*"))
				{
					foundNewPath = true;
				}


				childEntry = indexPage.getNext(childRid);
			}
			// **** End print of children.

			if(foundNewPath == false && fullKeyUsed == false)
			{
				//System.out.println("no more potential paths at: " + currentPath);
				String[] explored = currentPath.split("_\\*");
				//System.out.println(explored[0]);

				searchedNodes.add(explored[0]);
				lastVisited = currentPath;
				return nearestNeighbors;
			}
				

			//System.out.println("bucketkey: " + bucketKey+ " vs currentKey: "+ currentPath);

			PageId nextPageId = indexPage.getPageNoByKey(pathKey);

			if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
				unpinPage(currentPageId);
				System.out.println("⚠️ No child found at: " + currentPath);
				return nearestNeighbors;
			}

			if (i == keys.length - 1) {
				// 🌟 FULL PREFIX MATCH: Look up full bucket key -> should find leaf
				StringKey fullKey = new StringKey(currentPath);	//new StringKey(bucketKey);
				PageId leafPageId = indexPage.getPageNoByKey(fullKey);

				if (leafPageId == null || leafPageId.pid == INVALID_PAGE) {
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ No leaf linked to: " + currentPath);
					return nearestNeighbors;
				}

				// 🛡️ Pin the page and check if it's a real LEAF
				Page leafCandidatePage = pinPage(leafPageId);
				short leafNodeType = new LSHFBTSortedPage(leafCandidatePage, headerPage.get_keyType()).getType();

				if (leafNodeType != NodeType.LEAF) {
					// 🚨 Wrong node type: unpin and exit
					unpinPage(leafPageId);
					unpinPage(indexPage.getCurPage(), false);
					System.out.println("⚠️ Page under " + currentPath + " is not a LEAF (type: " + leafNodeType + ")");
					return nearestNeighbors;
				}

				// ✅ Now safely treat it as a leaf
				leafPage = new LSHFBTLeafPage(leafCandidatePage, AttrType.attrVector100D);

				unpinPage(indexPage.getCurPage(), false); // Done with index
				break; // Exit traversal
			}

			unpinPage(currentPageId);
			currentPageId = nextPageId;
		}

		//System.out.println("We followed the current path: " + currentPath);
		searchedNodes.add(currentPath);



		if (leafPage == null) {
			System.out.println("⚠️ Leaf page not found.");
			return nearestNeighbors;
		}

		// ✅ Now scan from the correct leaf page
		while (leafPage != null) {
			RID rid = new RID();
			KeyDataEntry entry = leafPage.getFirst(rid);

			while (entry != null) {

				double distance = query.computeDistance(query, ((Vector100DKey) entry.key).getKey());

				if(distance > highestDistanceFound)
					highestDistanceFound = distance;
				//System.out.println("distance: " + distance);

				if (distance <= range) {
					nearestNeighbors.add(entry);
				}

				//nearestNeighbors.add(entry);
				// if (nearestNeighbors.size() >= number_of_neighbors) {
				//     break;
				// }
				entry = leafPage.getNext(rid);
			}

			// if (nearestNeighbors.size() >= number_of_neighbors) {
			//     break;
			// }

			PageId rightSiblingId = leafPage.getNextPage();
			if (rightSiblingId.pid == INVALID_PAGE) {
				break;
			}

			unpinPage(leafPage.getCurPage(), false);
			leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
		}

		if (leafPage != null) {
			unpinPage(leafPage.getCurPage(), false);
		}

		return nearestNeighbors;
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
			IOException,
			IndexFullDeleteException
	{
		// ✅ Step 1: Start from root
		PageId currentPageId = headerPage.get_rootId();
		PageId leafPageId = null;
		if (currentPageId.pid == INVALID_PAGE) {
			LSHFBTLeafPage newLeafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
			PageId newRootPageId = newLeafPage.getCurPage();
			newLeafPage.setNextPage(new PageId(INVALID_PAGE));
			newLeafPage.setPrevPage(new PageId(INVALID_PAGE));
			unpinPage(newRootPageId, true);
			updateHeader(newRootPageId);
			return;
		}

		// Step 2: Traverse the index tree down to the parent of the final bucket key.
		// For a bucketKey like "layer0_6_6_23_23_7", we want to traverse for:
		// "layer0", "layer0_6", "layer0_6_6", "layer0_6_6_23", "layer0_6_6_23_23"
		String[] keys = bucketKey.split("_");
		String currentPath = keys[0];
		LSHFBTIndexPage indexPage = null;
		// Loop from 1 to keys.length - 1 (so that the last iteration corresponds to the final bucket key).
		for (int i = 1; i < keys.length; i++) {
			currentPath += "_" + keys[i];
			StringKey pathKey = new StringKey(currentPath);
			
			Page page = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(page, headerPage.get_keyType()).getType();
			if (nodeType != NodeType.INDEX) {
				throw new NodeNotMatchException(null, "Expected INDEX node but found different type while traversing: " + currentPath);
			}
			indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
			
			// If this is the final iteration then we are at the parent responsible for the full key.
			if (i == keys.length - 1) {
				// Do not descend further.
				unpinPage(currentPageId);
				break;
			}
			
			// For intermediate prefixes, we must have an existing branch.
			PageId nextPageId = indexPage.getPageNoByKey(pathKey);
			if (nextPageId == null || nextPageId.pid == INVALID_PAGE) {
				throw new InsertException(null, "Bucket path not fully built at intermediate: " + currentPath);
			}
			unpinPage(currentPageId);
			currentPageId = nextPageId;
		}

		// Step 3: At the parent node for the full bucket key.
		// Now check for the full key in this index node with an exact match.
		Page page = pinPage(currentPageId);
		indexPage = new LSHFBTIndexPage(page, headerPage.get_keyType());
		StringKey fullKey = new StringKey(bucketKey);
		leafPageId = indexPage.getPageNoByKey(fullKey);

		// EXACT MATCH CHECK: iterate over entries in indexPage to see if an entry exactly matches fullKey.
		boolean exactMatch = false;
		if (leafPageId != null && leafPageId.pid != INVALID_PAGE) {
			RID localRid = new RID();
			KeyDataEntry entry = indexPage.getFirst(localRid);
			while (entry != null) {
				if (entry.key.toString().equals(fullKey.getKey())) {
					exactMatch = true;
					break;
				}
				entry = indexPage.getNext(localRid);
			}
		}
		if (!exactMatch) {
			// Treat as if no leaf exists.
			leafPageId = new PageId(INVALID_PAGE);
		}

		//System.out.println("🌟 [BUCKET DEBUG] Checking for full bucket key: " + fullKey.getKey());
		// if (leafPageId != null && leafPageId.pid != INVALID_PAGE) {
		// 	System.out.println("🚨 [BUCKET WARNING] Bucket " + fullKey.getKey() + " already exists, PageId=" + leafPageId.pid);
		// } else {
		// 	System.out.println("✅ [BUCKET OK] Bucket " + fullKey.getKey() + " is fresh, will create new leaf.");
		// }

		if (leafPageId == null || leafPageId.pid == INVALID_PAGE) {
			// No leaf exists for the full key: create a new leaf.
			//System.out.println("⚡ [INSERT DEBUG] Creating LEAF NODE for: " + fullKey.getKey());
			LSHFBTLeafPage newLeafPage = new LSHFBTLeafPage(AttrType.attrVector100D);
			leafPageId = newLeafPage.getCurPage();
			//System.out.println("🌿 [LEAF CREATED] Bucket Key: " + fullKey.getKey() + " -> Leaf PageId: " + leafPageId.pid);
			// Insert this leaf into the parent index node.
			indexPage.insertKey(fullKey, leafPageId);
			unpinPage(newLeafPage.getCurPage(), true);
		} else {
			// A leaf exists; verify its type.
			Page leafCandidatePage = pinPage(leafPageId);
			short candidateType = new LSHFBTSortedPage(leafCandidatePage, headerPage.get_keyType()).getType();
			if (candidateType != NodeType.LEAF) {
				throw new InsertException(null, "Bucket key maps to non-leaf page: " + bucketKey);
			}
			unpinPage(leafPageId, false);
		}
		unpinPage(indexPage.getCurPage(), true); // Done with final index.

		// Step 4: Insert the record into the leaf.
		Page leafPagePinned = pinPage(leafPageId);
		LSHFBTLeafPage leafPage = new LSHFBTLeafPage(leafPagePinned, AttrType.attrVector100D);

		int recordCount = 0;
		RID countRid = new RID();
		KeyDataEntry entry = leafPage.getFirst(countRid);
		while (entry != null) {
			recordCount++;
			entry = leafPage.getNext(countRid);
		}

		if (recordCount < 38) {
			leafPage.insertRecord(key, rid);
			unpinPage(leafPageId, true);
			return;
		}

		// Step 5: Handle leaf page overflow.
		PageId rightSiblingId = leafPage.getNextPage();
		while (true) {
			if (rightSiblingId.pid != INVALID_PAGE) {
				unpinPage(leafPageId, false);
				leafPage = new LSHFBTLeafPage(pinPage(rightSiblingId), AttrType.attrVector100D);
				leafPageId = rightSiblingId;
			} else {
				LSHFBTLeafPage newLeaf = new LSHFBTLeafPage(AttrType.attrVector100D);
				PageId newLeafPageId = newLeaf.getCurPage();
				leafPage.setNextPage(newLeafPageId);
				newLeaf.setPrevPage(leafPage.getCurPage());
				unpinPage(leafPage.getCurPage(), true);
				leafPage = newLeaf;
				leafPageId = newLeafPageId;
			}
			
			recordCount = 0;
			entry = leafPage.getFirst(countRid);
			while (entry != null) {
				recordCount++;
				entry = leafPage.getNext(countRid);
			}
			
			if (recordCount < 38) {
				leafPage.insertRecord(key, rid);
				unpinPage(leafPageId, true);
				return;
			}
			
			rightSiblingId = leafPage.getNextPage();
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
    
		// Validate key length and type.
		if (LSHFBT.getKeyLength(key) > headerPage.get_maxKeySize())
			throw new KeyTooLongException(null, "");
		if (!(key instanceof StringKey))
			throw new KeyNotMatchException(null, "Only StringKeys allowed for internal nodes.");

		// Extract the bucket key (e.g. "layer0_23_56_21") and split it.
		// Extract the bucket key (e.g. "layer0_23_56_21") and split it.
		String bucketKey = ((StringKey) key).getKey();
		String[] keyParts = bucketKey.split("_");

		// Step 1: Start at the root.
		PageId currentPageId = headerPage.get_rootId();
		LSHFBTIndexPage currentIndexPage = null;

		// If the tree is empty, create the very first index node as the root.
		if (currentPageId.pid == INVALID_PAGE) {
			currentIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
			PageId newRootPageId = currentIndexPage.getCurPage();
			updateHeader(newRootPageId);
			headerPage.set_rootId(newRootPageId);
			currentPageId = newRootPageId;
			unpinPage(newRootPageId, true);
		} else {
			// Otherwise, if the current root is of type BTHEAD, convert it to an INDEX.
			Page currentPage = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(currentPage, headerPage.get_keyType()).getType();
			if (nodeType == NodeType.BTHEAD) {
				currentIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
				PageId newIndexPageId = currentIndexPage.getCurPage();
				updateHeader(newIndexPageId);
				headerPage.set_rootId(newIndexPageId);
				currentPageId = newIndexPageId;
				unpinPage(newIndexPageId, true);
			} else {
				unpinPage(currentPageId);
			}
		}

		// Step 2: Traverse the bucket key and create index nodes for every unique cumulative prefix.
		// For example, if bucketKey = "layer0_-10_-2_-1_-8_-2" we want branch nodes for:
		// "layer0", "layer0_-10", "layer0_-10_-2", "layer0_-10_-2_-1", "layer0_-10_-2_-1_-8"
		// The full key ("layer0_-10_-2_-1_-8_-2") will be handled later in insertLeaf().
		String currentPath = keyParts[0]; // First part, e.g. "layer0"
		// Loop from 1 to keyParts.length-1 so that the final part is not handled here.
		for (int i = 1; i < keyParts.length - 1; i++) {
			currentPath += "_" + keyParts[i];
			StringKey pathKey = new StringKey(currentPath);
			
			// Pin the current index node.
			Page currentPage = pinPage(currentPageId);
			short nodeType = new LSHFBTSortedPage(currentPage, headerPage.get_keyType()).getType();
			if (nodeType != NodeType.INDEX)
				throw new NodeNotMatchException(null, "Expected INDEX node but found different type at " + currentPath);
			currentIndexPage = new LSHFBTIndexPage(currentPage, headerPage.get_keyType());
			
			// Look up the cumulative prefix.
			PageId nextPageId = currentIndexPage.getPageNoByKey(pathKey);
			
			// Check for an exact match in the current index node:
			boolean exactFound = false;
			if (nextPageId != null && nextPageId.pid != INVALID_PAGE) {
				RID localRid = new RID();
				KeyDataEntry entry = currentIndexPage.getFirst(localRid);
				while (entry != null) {
					// Compare the stored key (using toString()) with pathKey.
					if (entry.key.toString().equals(pathKey.getKey())) {
						exactFound = true;
						break;
					}
					entry = currentIndexPage.getNext(localRid);
				}
			}
			// If either nextPageId is null/invalid or no exact match found, force creation.
			if (!exactFound) {
				//System.out.println("⚡ [INSERT DEBUG] Creating INDEX NODE for: " + currentPath);
				LSHFBTIndexPage newIndexPage = new LSHFBTIndexPage(headerPage.get_keyType());
				nextPageId = newIndexPage.getCurPage();
				// Insert mapping for the exact key.
				currentIndexPage.insertKey(pathKey, nextPageId);
				// Unpin the newly created node.
				unpinPage(newIndexPage.getCurPage(), true);
			} else {
				// Otherwise, ensure that the node is not a leaf.
				Page nextPage = pinPage(nextPageId);
				short nextNodeType = new LSHFBTSortedPage(nextPage, headerPage.get_keyType()).getType();
				if (nextNodeType == NodeType.LEAF)
					throw new InsertException(null, "Corruption: expected INDEX but found LEAF at " + currentPath);
				unpinPage(nextPageId);
			}
			
			// Unpin the current index page and update currentPageId for the next iteration.
			unpinPage(currentPageId);
			currentPageId = nextPageId;
		}

		// At this point the index (branch) structure has been created exactly
		// for all intermediate prefixes.
		// The complete bucket key (the final part) will be handled separately (for leaf creation).

		
		// At this point the branch (index) structure is built up to the full bucket key.
		// The responsibility for inserting the actual record into a leaf node
		// will be handled in insertLeaf().
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
 
 
 