package iterator;

import global.*;
import heap.*;
import index.*;
import java.util.*;
import lshfindex.*;

public class INLJoins {
    private ArrayList<ArrayList> resultTuples = new ArrayList<ArrayList>();

    @SuppressWarnings("unchecked")
    public INLJoins(
        String relName2,String dbpath,int qfo,int queryField, int distance,int numBuf, String useLSH, 
        ArrayList<ArrayList> table, String[] parts, ArrayList<Object> columns
    ) throws Exception {
            SystemDefs sysdef = new SystemDefs( dbpath, 0, numBuf, "Clock" );
            Heapfile heapfile = new Heapfile("data_heap.in");

            FldSpec[] sc_projlist = new FldSpec[1];
            RelSpec sc_rel = new RelSpec(RelSpec.outer); 
            sc_projlist[0] = new FldSpec(sc_rel, 1);
            FileScan sc_scan = null;
            try {
                sc_scan = new FileScan("sc_heap.in", new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30}, (short) 1, 1, sc_projlist, null);
            }
            catch (Exception e) {
            e.printStackTrace();
            }
            Tuple t =new Tuple();
            try {
                t = sc_scan.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }            
            AttrType[] _schema = null;
            String schemaString = "";
            short _len_in1 = 0;
            if (t != null) {
                schemaString = t.getStrFld(1);
                _len_in1 = (short) (schemaString.length()-2);
                _schema = new AttrType[schemaString.length()-2];
                for (int i = 0; i < schemaString.length()-2; i++) {
                    int typeCode = schemaString.charAt(i) - '0';
                    switch (typeCode) {
                        case 1: _schema[i] = new AttrType(AttrType.attrInteger); break;
                        case 2: _schema[i] = new AttrType(AttrType.attrReal); break;
                        case 3: _schema[i] = new AttrType(AttrType.attrString); break;
                        case 4: _schema[i] = new AttrType(AttrType.attrVector100D); break;
                        default: throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
                    }
                }
            }
            int L = schemaString.charAt(schemaString.length()-1) - '0';
            int h = schemaString.charAt(schemaString.length()-2) - '0';
            while (t != null) {
                try {
                    t = sc_scan.get_next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sc_scan.close();
            // String targetVectorFile = parts[1].trim() + ".txt";
            // System.out.println(targetVectorFile);
            int rangeThreshold = distance;
            
            // System.out.println(targetVector);
            
            // ArrayList<Integer> columns = new ArrayList<>();

            // for (int i = 4; i < parts.length; i++) {
            //     columns.add(Integer.parseInt(parts[i].trim()));
            // }
            int vectorIndex = columns.indexOf(queryField);
            int noOutFlds = parts.length - 3;
            FldSpec[] projlist = new FldSpec[noOutFlds];
            RelSpec rel = new RelSpec(RelSpec.outer); 
            for (int i = 0; i < noOutFlds; i++) {
                projlist[i] = new FldSpec(rel, Integer.parseInt(parts[3+i].trim()));
            }
            RSIndexScan scan = null;
            for(ArrayList<Object> row:table){
                 String values = (String) row.get(vectorIndex);
                 values = values.replace("[", "").replace("]", "").trim();
                 String[] val = values.split(",");
                 short[] vector = new short[100];
                 for (int i = 0; i < 100; i++) {
                     vector[i] = Short.parseShort(val[i].trim());
                 }
                 Vector100Dtype targetVector =  new Vector100Dtype(vector);
                 if(useLSH.equals("H")){
                    System.out.println("Hi");
                    try{     
                            scan = new RSIndexScan(
                            new IndexType(IndexType.LSHF_Index), "data_heap.in",
                            relName2 + '_' + queryField + '_' + h + '_' + L,
                            _schema, getStringSizes(_schema), _len_in1, noOutFlds,  projlist, null, queryField, targetVector, rangeThreshold
                        );
                    }catch(Exception e){
                        e.printStackTrace();
                    }
        
            }
            else{
                try{     
                    scan = new RSIndexScan(
                    new IndexType(IndexType.None), "data_heap.in","",
                    _schema, getStringSizes(_schema), _len_in1, noOutFlds,  projlist, null, queryField, targetVector, rangeThreshold
                );
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
            List<Tuple> results = new ArrayList<>();
            results = scan.get_all_results();
            // System.out.println(results);
            AttrType[] out_types = new AttrType[noOutFlds];
            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = _schema[projlist[i].offset - 1];
                    }
                result.print(out_types);
                ArrayList<Object> joined = new ArrayList<>(row);
                joined.addAll(result.copy(out_types));
                System.out.println(joined);
                resultTuples.add(joined);
            }

    }


}
private static short[] getStringSizes(AttrType[] schema) {
    int count = 0;
    for (AttrType attr : schema) {
        if (attr.attrType == AttrType.attrString) count++;
    }
    short[] strSizes = new short[count];
    Arrays.fill(strSizes, (short) 30); // Default string length
    return strSizes;
}

public ArrayList<ArrayList> get_all_results() {
    return resultTuples;
}
}
