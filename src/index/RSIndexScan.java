package index;
import global.*;
import heap.*;
import iterator.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RSIndexScan {
    private List<RID> rangeResults;  // List of RIDs from rangeSearch
    private Heapfile f;
    private List<Tuple> resultTuples;
    private FldSpec[] perm_mat;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private int _fldNum; 

    public RSIndexScan(
            IndexType index,        
            final String relName,  
            final String indName,  
            AttrType types[],      
            short str_sizes[],     
            int noInFlds,          
            int noOutFlds,         
            FldSpec outFlds[],     
            CondExpr selects[],  
            final int fldNum,  
            Vector100Dtype query, 
            int distance
    ) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException, IOException {

        _noInFlds = noInFlds;
        _noOutFlds = noOutFlds;
        _types = types;
        _s_sizes = str_sizes;
        _selects = selects;
        perm_mat = outFlds;
        _fldNum = fldNum;  
        FileScan fscan = null;
        resultTuples = new ArrayList<>(); 
        Sort sort = null;

        FldSpec[] inFlds = new FldSpec[noInFlds];
        RelSpec rel = new RelSpec(RelSpec.outer); 
        for (int i = 0; i < noInFlds; i++) {
            inFlds[i] = new FldSpec(rel, i + 1);
        }

        if (index.indexType == IndexType.LSHF_Index) {
            // Use LSHF Index for nearest neighbor search
            try {
                //LSHFFile lshIndex = new LSHFFile(indName);
                //RSResults = lshIndex.rangeSearch(query, count);
                processResults();
            } catch (Exception e) {
                throw new IndexException(e, "RSIndexScan: LSHFFile RS search failed");
            }
        } else if (index.indexType == IndexType.None) {
            try {
                fscan = new FileScan(relName, _types, _s_sizes, (short) _noInFlds, _noInFlds, inFlds, _selects);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            try {
                sort = new Sort(types, (short) _noInFlds, _s_sizes, fscan, fldNum, new TupleOrder(TupleOrder.Ascending), 30, 1024, query, 0);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Tuple t = new Tuple();

            try {
            t = sort.get_next();
            }
            catch (Exception e) {
            e.printStackTrace(); 
            }
            while (t != null) {
                try {
                    if (query.computeDistance(t.get100DVectorFld(fldNum), query) <= distance) {
                        Tuple newTuple = new Tuple(t);
                        Tuple projectedTuple = new Tuple();
                        AttrType[] out_types = new AttrType[noOutFlds];
                        for (int i = 0; i < noOutFlds; i++) {
                            out_types[i] = types[perm_mat[i].offset - 1];
                        }
                        projectedTuple.setHdr((short) noOutFlds, out_types, _s_sizes);


                        Projection.Project(newTuple, types, projectedTuple, perm_mat, noOutFlds);
                        resultTuples.add(projectedTuple);
                        //resultTuples.add(newTuple);
                    }
                    t = sort.get_next();
                } catch (Exception e) {
                e.printStackTrace(); 
                break;
                }
            }
            try {
                sort.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new UnknownIndexTypeException("RSIndexScan: Unsupported index type");
        }
    }

    /**
     * Process all results in one call and store them in resultTuples
     */
    private void processResults() throws IndexException, IOException {
        for (RID rid : rangeResults) {
            Tuple tuple;
            try {
                tuple = f.getRecord(rid);
                tuple.setHdr((short) _noInFlds, _types, _s_sizes);
            } catch (Exception e) {
                throw new IndexException(e, "RSIndexScan: Failed to retrieve record");
            }

            // Apply selection conditions
            boolean eval;
            try {
                eval = PredEval.Eval(_selects, tuple, null, _types, null);
            } catch (Exception e) {
                throw new IndexException(e, "RSIndexScan: Predicate evaluation failed");
            }

            if (eval) {
                // Project the necessary fields and store
                try {
                    Tuple projectedTuple = new Tuple();
                    projectedTuple.setHdr((short) _noOutFlds, _types, _s_sizes);
                    Projection.Project(tuple, _types, projectedTuple, perm_mat, _noOutFlds);
                    resultTuples.add(projectedTuple);
                } catch (Exception e) {
                    throw new IndexException(e, "RSIndexScan: Projection failed");
                }
            }
        }
    }

    public List<Tuple> get_all_results() {
        return resultTuples;
    }

    public void close() {
        rangeResults = null;
        resultTuples = null;
    }
}
