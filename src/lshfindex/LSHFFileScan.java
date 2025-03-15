package lshfindex;

import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

import java.io.IOException;

public class LSHFFileScan extends IndexFileScan {
private LSHFBTreeFile btree;
    private Queue<PageId> traversalQueue;  // Queue for BFS traversal
    private boolean initialized;
    private LSHFBTLeafPage leafPage;
    private static final int INVALID_PAGE = -1;
    ArrayList<KeyDataEntry> AllRecords;

    public LSHFFileScan(LSHFBTreeFile btree) throws IOException, ConstructPageException, PinPageException, UnpinPageException {
        this.btree = btree;
        this.traversalQueue = new LinkedList<>();
        this.initialized = false;
    }

    public void initialize() throws IOException, ConstructPageException, PinPageException, UnpinPageException, KeyNotMatchException, IteratorException, IndexSearchException {
        if (!initialized) {
            //System.out.println("🔍 Initializing LSHFFileScan...");
            PageId rootId = btree.getHeaderPage().get_rootId();
            if (rootId != null && rootId.pid != -1) {
                traversalQueue.add(rootId);  // Start BFS from the root
            }
            this.initialized = true;
        }
    }

    public ArrayList<KeyDataEntry> LSHFFileScan() throws IOException, KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, ScanIteratorException, IndexSearchException {
        if (!initialized) initialize();

        AllRecords = new ArrayList<>();

        //System.out.println("🔎 Scanning Full BTree Structure...");
        while (!traversalQueue.isEmpty()) {
            PageId currentId = traversalQueue.poll();
            Page currentPage = btree.pinPage(currentId);
            LSHFBTSortedPage sortedPage = new LSHFBTSortedPage(currentPage, btree.getHeaderPage().get_keyType());

            if (sortedPage.getType() == NodeType.INDEX) {
                //System.out.println("🔹 Internal Node: " + currentId.pid);
                LSHFBTIndexPage indexPage = new LSHFBTIndexPage(currentPage, btree.getHeaderPage().get_keyType());
                
                // Add all child pages to traversal queue
                RID rid = new RID();
                KeyDataEntry entry = indexPage.getFirst(rid);
                while (entry != null) {
                    PageId childPageId = ((IndexData) entry.data).getData();
                    if (childPageId != null && childPageId.pid != -1) {
                        traversalQueue.add(childPageId);
                    }
                    entry = indexPage.getNext(rid);
                }

            } else if (sortedPage.getType() == NodeType.LEAF) {
                //System.out.println("📌 Leaf Node Found at Page: " + currentId.pid);
                LSHFBTLeafPage leafPage = new LSHFBTLeafPage(currentPage, AttrType.attrVector100D);

                printLeafEntries(leafPage);
                
                //  Traverse right sibling links
                PageId nextLeafId = leafPage.getNextPage();
                while (nextLeafId.pid != INVALID_PAGE) {
                    //System.out.println("➡️ Moving to next leaf: " + nextLeafId.pid);
                    LSHFBTLeafPage nextLeaf = new LSHFBTLeafPage(btree.pinPage(nextLeafId), AttrType.attrVector100D);
                    printLeafEntries(nextLeaf);
                    
                    // Move to next right sibling
                    nextLeafId = nextLeaf.getNextPage();
                    
                    // Unpin this leaf before moving
                    btree.unpinPage(nextLeaf.getCurPage());
                }
            }

            btree.unpinPage(currentId);
        }
        //System.out.println("✅ Full BTree Scan Completed.");

        return AllRecords;
    }

    private void printLeafEntries(LSHFBTLeafPage leafPage) throws IOException, IteratorException {
        //System.out.println("📄 Leaf Page " + leafPage.getCurPage().pid + " Entries:");
        RID rid = new RID();
        KeyDataEntry entry = leafPage.getFirst(rid);
        while (entry != null) {
            //System.out.println("✅ Leaf Record: " + entry.key);
            AllRecords.add(entry);
            entry = leafPage.getNext(rid);

            
        }
    }

    @Override
    public KeyDataEntry get_next() throws ScanIteratorException, IteratorException, IOException {
        if (leafPage == null) {
            return null;
        }

        RID rid = new RID();
        KeyDataEntry entry = leafPage.getNext(rid);

        if (entry == null) {
            // Move to the next leaf page
            PageId nextPageId = leafPage.getNextPage();
            if (nextPageId.pid == -1) {
                return null; // End of scan
            }
            try {
                btree.unpinPage(leafPage.getCurPage());
                Page nextPage = btree.pinPage(nextPageId);
                leafPage = new LSHFBTLeafPage(nextPage, AttrType.attrVector100D);
                return leafPage.getFirst(rid);
            } catch (Exception e) {
                throw new ScanIteratorException(e, "Error moving to next leaf page");
            }
        }
        return entry;
    }

    @Override
    public int keysize() {
        return 4;  // Adjust this value based on actual key size in bytes
    }

    public void delete_current()
    {
        System.out.println("Not implmented");
    }
}