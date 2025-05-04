package iterator;

import global.*;
import heap.*;
import index.*;
import java.util.*;
import lshfindex.*;



public class INLJoins {
    private ArrayList<ArrayList> resultTuples = new ArrayList<ArrayList>();
    AttrType[] schema;
    int numAttributes;
    @SuppressWarnings("unchecked")
    public INLJoins(
        String relName2,int qfo,int queryField, int distance, String useLSH, 
        ArrayList<ArrayList> table, String[] parts, ArrayList<Object> columns
    ) throws Exception {
            // SystemDefs sysdef = new SystemDefs( dbpath, 0, numBuf, "Clock" );
            // Heapfile heapfile = new Heapfile("data_heap.in");

            // FldSpec[] sc_projlist = new FldSpec[1];
            // RelSpec sc_rel = new RelSpec(RelSpec.outer); 
            // sc_projlist[0] = new FldSpec(sc_rel, 1);
            // FileScan sc_scan = null;
            // try {
            //     sc_scan = new FileScan("sc_heap.in", new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30}, (short) 1, 1, sc_projlist, null);
            // }
            // catch (Exception e) {
            // e.printStackTrace();
            // }
            // Tuple t =new Tuple();
            // try {
            //     t = sc_scan.get_next();
            // } catch (Exception e) {
            //     e.printStackTrace();
            // }            
            // AttrType[] _schema = null;
            // String schemaString = "";
            // short _len_in1 = 0;
            // if (t != null) {
            //     schemaString = t.getStrFld(1);
            //     _len_in1 = (short) (schemaString.length()-2);
            //     _schema = new AttrType[schemaString.length()-2];
            //     for (int i = 0; i < schemaString.length()-2; i++) {
            //         int typeCode = schemaString.charAt(i) - '0';
            //         switch (typeCode) {
            //             case 1: _schema[i] = new AttrType(AttrType.attrInteger); break;
            //             case 2: _schema[i] = new AttrType(AttrType.attrReal); break;
            //             case 3: _schema[i] = new AttrType(AttrType.attrString); break;
            //             case 4: _schema[i] = new AttrType(AttrType.attrVector100D); break;
            //             default: throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
            //         }
            //     }
            // }
            // int L = schemaString.charAt(schemaString.length()-1) - '0';
            // int h = schemaString.charAt(schemaString.length()-2) - '0';
            // while (t != null) {
            //     try {
            //         t = sc_scan.get_next();
            //     } catch (Exception e) {
            //         e.printStackTrace();
            //     }
            // }
            // sc_scan.close();
            // // String targetVectorFile = parts[1].trim() + ".txt";
            // // System.out.println(targetVectorFile);
            int rangeThreshold = distance;
            
            // System.out.println(targetVector);
            
            // ArrayList<Integer> columns = new ArrayList<>();

            // for (int i = 4; i < parts.length; i++) {
            //     columns.add(Integer.parseInt(parts[i].trim()));
            // }
            try {
                // Initialize MiniBase
                //SystemDefs sysDef = new SystemDefs(dbPath, 0, bufferPages, "Clock");
    
                // Load schema from sc_heap.in
                Heapfile heapfile = new Heapfile("sc_"+relName2+".in");
                Scan scan1 = heapfile.openScan();
                Tuple tuple1;
                RID rid1 = new RID();
                tuple1 = scan1.getNext(rid1);
                tuple1.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                tuple1.print(new AttrType[]{new AttrType(AttrType.attrString)});
        
                String attributes = tuple1.getStrFld(1);
                char[] attr = attributes.toCharArray();
                schema = new AttrType[attr.length];
                numAttributes = attributes.length();
    
        
                for(int i = 0; i < attr.length; i++)
                {
                    int typeCode = Integer.parseInt(String.valueOf(attr[i]));
                    switch (typeCode) {
                        case 1: schema[i] = new AttrType(AttrType.attrInteger); break;
                        case 2: schema[i] = new AttrType(AttrType.attrReal); break;
                        case 3: schema[i] = new AttrType(AttrType.attrString); break;
                        case 4: schema[i] = new AttrType(AttrType.attrVector100D); break;
                        default: throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
                    }
        
                    //System.out.println("Schema: "+schema[i]);
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }

            Heapfile index_heapfile = new Heapfile(relName2 + "indexes");
            Scan scan = index_heapfile.openScan();
            //tuple;
            Tuple tuple;
            RID rid = new RID();
            int h=0;
            int L=0;
            while((tuple = scan.getNext(rid)) != null)
            {
                tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                String index_String = tuple.getStrFld(1);
                String[] index_parts = index_String.split("_");
                if (qfo == Integer.parseInt(index_parts[1])) {
                    h = Integer.parseInt(index_parts[3]);
                    L = Integer.parseInt(index_parts[2]);           
                }
            }

            int vectorIndex = columns.indexOf(queryField);
            int noOutFlds = parts.length - 3;
            FldSpec[] projlist = new FldSpec[noOutFlds];
            RelSpec rel = new RelSpec(RelSpec.outer); 
            for (int i = 0; i < noOutFlds; i++) {
                projlist[i] = new FldSpec(rel, Integer.parseInt(parts[3+i].trim()));
            }
            RSIndexScan rs = null;
            for(ArrayList<Object> row:table){
                 String values = (String) row.get(vectorIndex);
                 values = values.replace("[", "").replace("]", "").trim();
                 String[] val = values.split(",");
                 short[] vector = new short[100];
                 for (int i = 0; i < 100; i++) {
                     vector[i] = Short.parseShort(val[i].trim());
                 }
                 System.out.println("Vector: " + Arrays.toString(vector));
                 Vector100Dtype targetVector =  new Vector100Dtype(vector);


                 if(useLSH.equals("H")){
                    try{     
                            rs = new RSIndexScan(
                            new IndexType(IndexType.LSHF_Index), relName2+".in",
                            relName2 + '_' + qfo + '_' + L + '_' + h,
                            schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, qfo, targetVector, rangeThreshold
                        );
                    }catch(Exception e){
                        e.printStackTrace();
                    }
        
            }
            else{
                try{     
                    rs = new RSIndexScan(
                    new IndexType(IndexType.None), relName2+".in","",
                    schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, qfo, targetVector, rangeThreshold
                );
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
            List<Tuple> results = new ArrayList<>();
            results = rs.get_all_results();
            // System.out.println(results);
            AttrType[] out_types = new AttrType[noOutFlds];
            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = schema[projlist[i].offset - 1];
                    }
                result.print(out_types);
                ArrayList<Object> joined = new ArrayList<>(row);
                joined.addAll(result.copy(out_types));
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
