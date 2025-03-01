package lshfindex;
import java.io.*;
import global.*;
import heap.*;

/**
 * Base class for a index file scan
 */
public abstract class IndexFileScan 
{
  /**
   * Get the next record.
   * @exception ScanIteratorException error when iterating through the records
   * @return the KeyDataEntry, which contains the key and data
   */
  abstract public KeyDataEntry get_next()
    throws ScanIteratorException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException;

  /** 
   * Delete the current record.
   * @exception ScanDeleteException delete current record failed
   */
   abstract public void delete_current() 
     throws ScanDeleteException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception;

  /**
   * Returns the size of the key
   * @return the keysize
   */
  abstract public int keysize();
}
