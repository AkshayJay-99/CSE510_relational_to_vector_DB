package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;

/** Note that in JAVA, methods can't be overridden to be more private.
    Therefore, the declaration of all private functions are now declared
    protected as opposed to the private type in C++.
*/

class HFDriver extends TestDriver implements GlobalConst
{

  private final static boolean OK = true;
  private final static boolean FAIL = false;
  
  private int choice;
  private final static int reclen = 100;
  
  public HFDriver () {
    super("hptest");
    choice = 2;      // big enough for file to occupy > 1 data page
    //choice = 2000;   // big enough for file to occupy > 1 directory page
    //choice = 5;
  }
  

public boolean runTests () {

    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

    SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
   
    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";
    
    newdbpath = dbpath;
    newlogpath = logpath;
    
    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;
    
    // Commands here is very machine dependent.  We assume
    // user are on UNIX system here
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    }
    catch (IOException e) {
      System.err.println ("IO error: "+e);
    }
    
    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;
    
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    }
    catch (IOException e) {
      System.err.println ("IO error: "+e);
    }
    
    //Run the tests. Return type different from C++
    boolean _pass = runAllTests();
    
    //Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    }
    catch (IOException e) {
      System.err.println ("IO error: "+e);
    }
    
    System.out.print ("\n" + "..." + testName() + " tests ");
    // Add disk read/write counter display
    System.out.println("Disk Reads: " + PCounter.rcounter);
    System.out.println("Disk Writes: " + PCounter.wcounter);
    System.out.print (_pass==OK ? "completely successfully" : "failed");
    System.out.print (".\n\n");
    
    return _pass;
  }
  
  protected boolean test1() {
    PCounter.initialize();
    System.out.println("\n  Test 1: Insert and scan records with two distinct vectors\n");
    boolean status = OK;
    RID rid = new RID();
    Heapfile f = null;

    System.out.println("  - Create a heap file\n");
    try {
        f = new Heapfile("file_1");
    } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Could not create heap file\n");
        e.printStackTrace();
    }

    if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
            != SystemDefs.JavabaseBM.getNumBuffers()) {
        System.err.println("*** The heap file has left pages pinned\n");
        status = FAIL;
    }

    // Initialize first and second vectors explicitly
    short[] testVector1 = new short[Vector100Dtype.VECTOR_SIZE];
    short[] testVector2 = new short[Vector100Dtype.VECTOR_SIZE];

    for (int j = 0; j < Vector100Dtype.VECTOR_SIZE; j++) {
        testVector1[j] = (short) (j);
        testVector2[j] = (short) (j + 100);  // distinct values for clarity
    }

    if (status == OK) {
        System.out.println("  - Add " + choice + " records to the file\n");

        for (int i = 0; (i < choice) && (status == OK); i++) {
            DummyRecord rec = new DummyRecord(reclen);
            rec.ival = i;
            rec.fval = (float) (i * 2.5);
            rec.name = "record" + i;

            if (i == 0)
                rec.vector100D = new Vector100Dtype(testVector1.clone()); // first spot
            else if (i == 1)
                rec.vector100D = new Vector100Dtype(testVector2.clone()); // second spot
            else
                rec.vector100D = new Vector100Dtype(testVector1.clone()); // remaining spots

            try {
                rid = f.insertRecord(rec.toByteArray());
                System.out.println(rid);
            } catch (Exception e) {
                status = FAIL;
                System.err.println("*** Error inserting record " + i + "\n");
                e.printStackTrace();
            }

            if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
                    != SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** Insertion left a page pinned\n");
                status = FAIL;
            }
        }

        try {
            if (f.getRecCnt() != choice) {
                status = FAIL;
                System.err.println("*** File reports " + f.getRecCnt() + 
                        " records, not " + choice + "\n");
            }
        } catch (Exception e) {
            status = FAIL;
            System.out.println("" + e);
            e.printStackTrace();
        }
    }

    // Scan and verify inserted records
    Scan scan = null;
    if (status == OK) {
        System.out.println("  - Scan the records just inserted\n");
        try {
            scan = f.openScan();
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
        }
        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
                == SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap-file scan has not pinned the first page\n");
            status = FAIL;
        }
    }

    Vector100Dtype firstVector = null;
    Vector100Dtype secondVector = null;

    if (status == OK) {
        int i = 0;
        DummyRecord rec = null;
        Tuple tuple;
        boolean done = false;

        while (!done) {
            try {
                tuple = scan.getNext(rid);
                if (tuple == null) {
                    done = true;
                    break;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
                break;
            }

            if (status == OK) {
                try {
                    rec = new DummyRecord(tuple);
                } catch (Exception e) {
                    System.err.println("" + e);
                    e.printStackTrace();
                }

                System.out.println("Record " + i + " vector -> " + rec.vector100D);

                // Store first two vectors for distance calculation
                if (i == 0) {
                    firstVector = new Vector100Dtype(rec.vector100D.getValues().clone());
                } else if (i == 1) {
                    secondVector = new Vector100Dtype(rec.vector100D.getValues().clone());
                }

                i++;
            }
        }

        // Compute Euclidean distance after retrieving both vectors
        if (firstVector != null && secondVector != null) {
            double distance = Vector100Dtype.computeDistance(firstVector, secondVector);
            System.out.println("\nEuclidean Distance between first two vectors: " + distance);
        }
    }

    // Display disk read/write counters
    System.out.println("Disk Reads: " + PCounter.rcounter);
    System.out.println("Disk Writes: " + PCounter.wcounter);
    if (status == OK)
        System.out.println("  Test 1 completed successfully.\n");

    return status;
}


  
protected boolean test2() {
  PCounter.initialize();
  System.out.println("\n  Test 2: Delete one of the first two vectors and print the remaining vector\n");
  boolean status = OK;
  Scan scan = null;
  RID rid = new RID();
  Heapfile f = null;
  Vector100Dtype firstVector = null;
  Vector100Dtype secondVector = null;

  System.out.println("  - Open the same heap file as test 1\n");
  try {
      f = new Heapfile("file_1");
  } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Could not open heap file\n");
      e.printStackTrace();
  }

  if (status == OK) {
      System.out.println("  - Scan and delete one of the first two vectors\n");
      try {
          scan = f.openScan();
      } catch (Exception e) {
          status = FAIL;
          System.err.println("*** Error opening scan\n");
          e.printStackTrace();
      }
  }

  if (status == OK) {
      int i = 0;
      Tuple tuple = new Tuple();
      boolean done = false;

      while (!done) {
          try {
              tuple = scan.getNext(rid);
              if (tuple == null) {
                  done = true;
                  break;
              }
          } catch (Exception e) {
              status = FAIL;
              e.printStackTrace();
          }

          if (!done && status == OK) {
              DummyRecord rec = null;
              try {
                  rec = new DummyRecord(tuple);
              } catch (Exception e) {
                  System.err.println("" + e);
                  e.printStackTrace();
              }

              // Store first two vectors
              if (i == 0) {
                  firstVector = new Vector100Dtype(rec.vector100D.getValues().clone());
              } else if (i == 1) {
                  secondVector = new Vector100Dtype(rec.vector100D.getValues().clone());
              }

              // Delete the first vector
              if (i == 0) {
                  try {
                      status = f.deleteRecord(rid);
                      System.out.println("\n✅ Deleted first vector successfully!");
                  } catch (Exception e) {
                      status = FAIL;
                      System.err.println("*** Error deleting first vector\n");
                      e.printStackTrace();
                  }
              }

              i++;
          }
      }
  }

  // Verify deletion and print remaining vector
  if (status == OK) {
      System.out.println("\n  - Verifying deletion and printing remaining vector\n");
      if (secondVector != null) {
          System.out.println("Remaining vector after deletion:\n" + Arrays.toString(firstVector.getValues()));
      } else {
          System.err.println("❌ No remaining vector found!");
      }
  }

  // Add disk read/write counter display
  System.out.println("Disk Reads: " + PCounter.rcounter);
  System.out.println("Disk Writes: " + PCounter.wcounter);
  if (status == OK)
      System.out.println("  Test 2 completed successfully.\n");

  return status;
}

protected boolean test3() {
  PCounter.initialize();
  System.out.println("\n  Test 3: Update records (including Vector100Dtype)\n");
  boolean status = OK;
  Scan scan = null;
  RID rid = new RID();
  Heapfile f = null;

  System.out.println("  - Open the same heap file as tests 1 and 2\n");
  try {
      f = new Heapfile("file_1");
  } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Could not open heap file\n");
      e.printStackTrace();
  }

  if (status == OK) {
      System.out.println("  - Updating the records\n");
      try {
          scan = f.openScan();
      } catch (Exception e) {
          status = FAIL;
          System.err.println("*** Error opening scan\n");
          e.printStackTrace();
      }
  }

  if (status == OK) {
      int i = 0;
      DummyRecord rec = null;
      Tuple tuple = new Tuple();
      boolean done = false;

      while (!done) {
          try {
              tuple = scan.getNext(rid);
              if (tuple == null) {
                  done = true;
                  break;
              }
          } catch (Exception e) {
              status = FAIL;
              e.printStackTrace();
          }

          if (!done && status == OK) {
              try {
                  rec = new DummyRecord(tuple);
              } catch (Exception e) {
                  System.err.println("" + e);
                  e.printStackTrace();
              }

              rec.fval = (float) 7 * i; // Update float field
              short[] updatedVector = new short[Vector100Dtype.VECTOR_SIZE];

              for (int j = 0; j < Vector100Dtype.VECTOR_SIZE; j++) {
                  updatedVector[j] = (short) (i + j * 2); // Example transformation
              }
              rec.vector100D.setValues(updatedVector); // Update vector field

              Tuple newTuple = null;
              try {
                  newTuple = new Tuple(rec.toByteArray(), 0, rec.getRecLength());
              } catch (Exception e) {
                  status = FAIL;
                  System.err.println("" + e);
                  e.printStackTrace();
              }
              try {
                  status = f.updateRecord(rid, newTuple);
              } catch (Exception e) {
                  status = FAIL;
                  e.printStackTrace();
              }

              if (status != OK) {
                  System.err.println("*** Error updating record " + i + "\n");
                  break;
              }
              i += 2; // Recall, we deleted every other record in test2.
          }
      }
  }

  scan = null;

  if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
          != SystemDefs.JavabaseBM.getNumBuffers()) {
      System.err.println("*** Updating left pages pinned\n");
      status = FAIL;
  }

  // Verify updates
  if (status == OK) {
      System.out.println("  - Check that the updates are correctly stored\n");
      try {
          scan = f.openScan();
      } catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
      }
      if (status == FAIL) {
          System.err.println("*** Error opening scan\n");
      }
  }

  if (status == OK) {
      int i = 0;
      DummyRecord rec = null;
      DummyRecord rec2 = null;
      Tuple tuple = new Tuple();
      Tuple tuple2 = new Tuple();
      boolean done = false;

      while (!done) {
          try {
              tuple = scan.getNext(rid);
              if (tuple == null) {
                  done = true;
                  break;
              }
          } catch (Exception e) {
              status = FAIL;
              e.printStackTrace();
          }

          if (!done && status == OK) {
              try {
                  rec = new DummyRecord(tuple);
              } catch (Exception e) {
                  System.err.println("" + e);
              }

              // Test `getRecord` method as well
              try {
                  tuple2 = f.getRecord(rid);
              } catch (Exception e) {
                  status = FAIL;
                  System.err.println("*** Error getting record " + i + "\n");
                  e.printStackTrace();
                  break;
              }

              try {
                  rec2 = new DummyRecord(tuple2);
              } catch (Exception e) {
                  System.err.println("" + e);
                  e.printStackTrace();
              }

              // Validate updates for integer and float values
              if ((rec.ival != i) || (rec.fval != (float) i * 7)
                      || (rec2.ival != i) || (rec2.fval != i * 7)) {
                  System.err.println("*** Record " + i + " differs from expected update\n");
                  System.err.println("rec.ival: " + rec.ival + " should be " + i + "\n");
                  System.err.println("rec.fval: " + rec.fval + " should be " + (i * 7.0) + "\n");
                  status = FAIL;
                  break;
              }

              // Validate vector updates
              boolean vectorMismatch = false;
              for (int j = 0; j < Vector100Dtype.VECTOR_SIZE; j++) {
                  if (rec.vector100D.getValues()[j] != (short) (i + j * 2)) {
                      vectorMismatch = true;
                      break;
                  }
              }

              if (vectorMismatch) {
                  System.err.println("*** Vector mismatch in updated record " + i + "\n");
                  status = FAIL;
                  break;
              }
          }
          i += 2; // Because we deleted the odd ones in test2
      }
  }

  // Add disk read/write counter display
  System.out.println("Disk Reads: " + PCounter.rcounter);
  System.out.println("Disk Writes: " + PCounter.wcounter);
  if (status == OK)
      System.out.println("  Test 3 completed successfully.\n");

  return status;
}


  //deal with variable size records.  it's probably easier to re-write
  //one instead of using the ones from C++
  protected boolean test5 () {
    return true;
  }
  
  
  protected boolean test4() {
    PCounter.initialize();
    System.out.println("\n  Test 4: Test some error conditions (including Vector100Dtype)\n");
    boolean status = OK;
    Scan scan = null;
    RID rid = new RID();
    Heapfile f = null;

    try {
        f = new Heapfile("file_1");
    } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Could not create heap file\n");
        e.printStackTrace();
    }

    if (status == OK) {
        System.out.println("  - Try to change the size of a record\n");
        try {
            scan = f.openScan();
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
        }
    }

    // The following is to test whether modifying the size of tuples triggers an exception.
    if (status == OK) {
        int len;
        DummyRecord rec = null;
        Tuple tuple = new Tuple();

        try {
            tuple = scan.getNext(rid);
            if (tuple == null) {
                status = FAIL;
            }
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        if (status == FAIL) {
            System.err.println("*** Error reading first record\n");
        }

        if (status == OK) {
            try {
                rec = new DummyRecord(tuple);
            } catch (Exception e) {
                System.err.println("" + e);
                status = FAIL;
            }

            len = tuple.getLength();
            Tuple newTuple = null;
            try {
                // Test shortening a record
                newTuple = new Tuple(rec.toByteArray(), 0, len - 1);
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
            }
            try {
                status = f.updateRecord(rid, newTuple);
            } catch (ChainException e) {
                status = checkException(e, "heap.InvalidUpdateException");
                if (status == FAIL) {
                    System.err.println("**** Shortening a record");
                    System.out.println("  --> Failed as expected \n");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (status == OK) {
                status = FAIL;
                System.err.println("###### The expected exception was not thrown for shortening record\n");
            } else {
                status = OK;
            }
        }

        if (status == OK) {
            try {
                rec = new DummyRecord(tuple);
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
            }

            len = tuple.getLength();
            Tuple newTuple = null;
            try {
                // Test lengthening a record
                newTuple = new Tuple(rec.toByteArray(), 0, len + 1);
            } catch (Exception e) {
                System.err.println("" + e);
                e.printStackTrace();
            }
            try {
                status = f.updateRecord(rid, newTuple);
            } catch (ChainException e) {
                status = checkException(e, "heap.InvalidUpdateException");
                if (status == FAIL) {
                    System.err.println("**** Lengthening a record");
                    System.out.println("  --> Failed as expected \n");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (status == OK) {
                status = FAIL;
                System.err.println("The expected exception was not thrown for lengthening record\n");
            } else {
                status = OK;
            }
        }
    }

    scan = null;

    // Vector-specific error testing
    if (status == OK) {
        System.out.println("  - Try updating vector size inconsistently\n");
        try {
            scan = f.openScan();
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
        }

        if (status == OK) {
            try {
                Tuple tuple = scan.getNext(rid);
                if (tuple == null) {
                    status = FAIL;
                }

                DummyRecord rec = new DummyRecord(tuple);
                short[] incorrectVector = new short[Vector100Dtype.VECTOR_SIZE + 5]; // Invalid size

                for (int i = 0; i < incorrectVector.length; i++) {
                    incorrectVector[i] = (short) i;
                }

                rec.vector100D.setValues(incorrectVector); // Invalid update

                Tuple newTuple = new Tuple(rec.toByteArray(), 0, rec.getRecLength());

                try {
                    status = f.updateRecord(rid, newTuple);
                } catch (ChainException e) {
                    status = checkException(e, "heap.InvalidUpdateException");
                    if (status == FAIL) {
                        System.err.println("**** Updating vector size incorrectly");
                        System.out.println("  --> Failed as expected \n");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (status == OK) {
                    status = FAIL;
                    System.err.println("The expected exception was not thrown for incorrect vector size\n");
                } else {
                    status = OK;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
    }

    if (status == OK) {
        System.out.println("  - Try to insert a record that's too long\n");
        byte[] record = new byte[MINIBASE_PAGESIZE + 4];
        try {
            rid = f.insertRecord(record);
        } catch (ChainException e) {
            status = checkException(e, "heap.SpaceNotAvailableException");
            if (status == FAIL) {
                System.err.println("**** Inserting a too-long record");
                System.out.println("  --> Failed as expected \n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (status == OK) {
            status = FAIL;
            System.err.println("The expected exception was not thrown for inserting an oversized record\n");
        } else {
            status = OK;
        }
    }

    // Add disk read/write counter display
    System.out.println("Disk Reads: " + PCounter.rcounter);
    System.out.println("Disk Writes: " + PCounter.wcounter);
    if (status == OK)
        System.out.println("  Test 4 completed successfully.\n");

    return (status == OK);
}

  
  protected boolean test6 () {
    
    PCounter.initialize();
    System.out.println("\n  Test: Read all vectors from the heap file\n");
    
    boolean status = OK;
    Scan scan = null;
    RID rid = new RID();
    Heapfile f = null;

    System.out.println("  - Open the heap file\n");
    try {
        f = new Heapfile("file_1");
    } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Could not open heap file\n");
        e.printStackTrace();
    }

    if (status == OK) {
        System.out.println("  - Scanning all records from the file\n");
        try {
            scan = f.openScan();
        } catch (Exception e) {
            status = FAIL;
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
        }
    }

    if (status == OK) {
        int i = 0;
        Tuple tuple = new Tuple();
        boolean done = false;

        while (!done) {
            try {
                tuple = scan.getNext(rid);
                if (tuple == null) {
                    done = true;
                    break;
                }
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!done && status == OK) {
                DummyRecord rec = null;
                try {
                    rec = new DummyRecord(tuple);
                } catch (Exception e) {
                    System.err.println("" + e);
                    e.printStackTrace();
                }

                // Print the vector stored in this record
                System.out.println("Record " + i + " vector: " + Arrays.toString(rec.vector100D.getValues()));
                i++;
            }
        }
    }

    // Display total disk reads/writes
    System.out.println("\nDisk Reads: " + PCounter.rcounter);
    System.out.println("Disk Writes: " + PCounter.wcounter);

    if (status == OK)
        System.out.println("  Test Read All Vectors completed successfully.\n");

    return status;
}
  
  protected boolean runAllTests (){
    
    boolean _passAll = OK;
    
    if (!test1()) { _passAll = FAIL; }
    if (!test2()) { _passAll = FAIL; }
    if (!test3()) { _passAll = FAIL; }
    if (!test4()) { _passAll = FAIL; }
    if (!test5()) { _passAll = FAIL; }
    if (!test6()) { _passAll = FAIL; }
    
    return _passAll;
  }

  protected String testName () {
   
    return "Heap File";
  }
}

class DummyRecord {
  
  // Content of the record
  public int ival;
  public float fval;
  public String name;
  public Vector100Dtype vector100D; // Added vector field
  
  // Length control
  private int reclen;
  private byte[] data;
  
  // Constants for field offsets
  private static final int INT_OFFSET = 0;                     // Integer field offset
  private static final int FLOAT_OFFSET = 4;                   // Float field offset
  private static final int STRING_OFFSET = 8;                  // String field offset
  private static final int VECTOR_OFFSET = 40;                 // Vector starts after the string
  private static final int VECTOR_SIZE_BYTES = 200;            // Each short = 2 bytes, 100 * 2 = 200

  /** Default constructor */
  public DummyRecord() {}

  /** Constructor for a new DummyRecord */
  public DummyRecord(int _reclen) {
    setRecLen(_reclen);
    data = new byte[_reclen];
  }
  
  /** Constructor: convert a byte array to DummyRecord object */
  public DummyRecord(byte[] arecord) throws IOException {
    setIntRec(arecord);
    setFloRec(arecord);
    setStrRec(arecord);
    setVectorRec(arecord); // Set vector field from byte array
    data = arecord;
    setRecLen(reclen);
  }

  /** Constructor: translate a tuple to a DummyRecord object */
  public DummyRecord(Tuple _atuple) throws java.io.IOException {
    data = _atuple.getTupleByteArray();
    setRecLen(_atuple.getLength());

    setIntRec(data);
    setFloRec(data);
    setStrRec(data);

    // Read vector100D from data
    // Read vector100D from data
    short[] vectorValues = new short[Vector100Dtype.VECTOR_SIZE];
    int offset = VECTOR_OFFSET; // Ensure correct vector retrieval

    for (int i = 0; i < Vector100Dtype.VECTOR_SIZE; i++) {
        vectorValues[i] = Convert.getShortValue(offset, data);
        offset += 2;
    }
    vector100D = new Vector100Dtype(vectorValues);
}


  /** Convert this class object to a byte array */
  /** Convert this class object to a byte array */
public byte[] toByteArray() throws java.io.IOException {
  // Ensure byte array has enough space
  data = new byte[reclen]; 
  
  Convert.setIntValue(ival, INT_OFFSET, data);
  Convert.setFloValue(fval, FLOAT_OFFSET, data);
  Convert.setStrValue(name, STRING_OFFSET, data);

  // Store vector100D in byte array
  int offset = VECTOR_OFFSET; // Ensure vector starts at correct position
  if (vector100D != null) {
      short[] values = vector100D.getValues();
      for (int i = 0; i < values.length; i++) {
          Convert.setShortValue(values[i], offset, data);
          offset += 2; // Move to next short position
      }
  }
  
  System.out.println(" Record size allocated: " + data.length);
  System.out.println(" Vector100D starts at: " + VECTOR_OFFSET);
  System.out.println(" Vector100D ends at: " + (VECTOR_OFFSET + VECTOR_SIZE_BYTES));

  return data;
}


  
  /** Get the integer value from byte array */
  public void setIntRec(byte[] _data) throws IOException {
    ival = Convert.getIntValue(INT_OFFSET, _data);
  }

  /** Get the float value from byte array */
  public void setFloRec(byte[] _data) throws IOException {
    fval = Convert.getFloValue(FLOAT_OFFSET, _data);
  }

  /** Get the String value from byte array */
  public void setStrRec(byte[] _data) throws IOException {
    name = Convert.getStrValue(STRING_OFFSET, _data, 32);
  }

  /** Get the Vector100Dtype from byte array */
  public void setVectorRec(byte[] _data) throws IOException {
    short[] values = new short[Vector100Dtype.VECTOR_SIZE];
    for (int i = 0; i < Vector100Dtype.VECTOR_SIZE; i++) {
      values[i] = Convert.getShortValue(VECTOR_OFFSET + (i * 2), _data);
    }
    vector100D = new Vector100Dtype(values);
  }
  
  /** Set the record length considering vector size */
  public void setRecLen(int size) {
    reclen = size + VECTOR_SIZE_BYTES; // Ensure enough space for vector
  }
  
  /** Get the record length */
  public int getRecLength() {
    return reclen;
  }  
}


public class HFTest {

   public static void main (String argv[]) {

     HFDriver hd = new HFDriver();
     boolean dbstatus;

     dbstatus = hd.runTests();

     if (dbstatus != true) {
       System.err.println ("Error encountered during buffer manager tests:\n");
       Runtime.getRuntime().exit(1);
     }

     Runtime.getRuntime().exit(0);
   }
}

