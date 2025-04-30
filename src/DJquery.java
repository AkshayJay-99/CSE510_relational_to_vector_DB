import global.*;
import heap.*;
import index.*;
import iterator.*;
import iterator.Iterator;

import java.io.*;
import java.util.*;

public class DJquery {
    
    public static class TupleListIterator extends Iterator {
        private final List<Tuple> tuples;
        private int index = 0;

        public TupleListIterator(List<Tuple> tuples) {
            this.tuples = tuples;
        }

        @Override
        public Tuple get_next() {
            if (index < tuples.size()) {
                return tuples.get(index++);
            } else {
                return null;
            }
        }

        @Override
        public void close() {
            tuples.clear();
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java DJquery <RELNAME1> <RELNAME2> <QSNAME> <NUMBUF>");
            return;
        }

        String relName1 = args[0];
        String relName2 = args[1];
        String qsFile = args[2];
        int numBuf = Integer.parseInt(args[3]);
        FileScan sc_scan = null;


        String dbpath = "/tmp/" + System.getProperty("user.name") + "."+relName1;
        try{
            SystemDefs sysdef = new SystemDefs( dbpath, 0, numBuf, "Clock" );
            Heapfile heapfile = new Heapfile("data_heap.in");

            FldSpec[] sc_projlist = new FldSpec[1];
            RelSpec sc_rel = new RelSpec(RelSpec.outer); 
            sc_projlist[0] = new FldSpec(sc_rel, 1);
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
            String queryLine;
            BufferedReader reader = new BufferedReader(new FileReader(qsFile));
            StringBuilder content = new StringBuilder();
            while ((queryLine = reader.readLine()) != null) {
                content.append(queryLine.trim());  // or just queryLine if you don't want trimming
            }
            reader.close();
            
            // Now you have all content in one String
            queryLine = content.toString();
            AttrType[] attrType = new AttrType[] {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrVector100D)
            };
            short[] strSizes = new short[] {30};

            processDistanceJoinQuery(relName1,relName2,h,L,queryLine,_schema, _len_in1, numBuf);
            flushPages();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private static void processDistanceJoinQuery(String relName1, String relName2,int h,int L, String queryLine,
    AttrType[] attrType, int attrSize, int numBuf) {
        try {
            // System.out.println(queryLine);
            String inside = queryLine.replace("DJOIN(", "");
            System.out.println(inside);
            int firstClose = inside.indexOf(")");
            String leftPart = inside.substring(0, firstClose + 1);
            String[] rightParts = inside.substring(firstClose + 2).replace(")","").split(",");
            // System.out.println(firstClose);
            System.out.println(leftPart);
            for (String right:rightParts){
                System.out.println(right);
            }
            int qf2 = Integer.parseInt(rightParts[0]);
            int distanceThreshold2 = Integer.parseInt(rightParts[1].trim());
            String useLSH2 = rightParts[2].trim();
            ArrayList<Integer> columns2 = new ArrayList<>();
            ArrayList<Object> columns = new ArrayList<>();

            for (int i = 3; i < rightParts.length; i++) {
                columns2.add(Integer.parseInt(rightParts[i].trim()));
            }
            // System.out.println(rightParts);
            int joinFieldOuter = Integer.parseInt(leftPart.substring(leftPart.indexOf('(')+1).split(",")[0].trim());
            int joinFieldInner = Integer.parseInt(rightParts[0].trim());
            int distanceThreshold = Integer.parseInt(rightParts[1].trim());
            String innerIndexOption = rightParts[2].trim();
            boolean useLSHInner = innerIndexOption.equalsIgnoreCase("H");
            int queryField;
            Iterator leftIter;
            ArrayList<ArrayList> table = new ArrayList<>();
            if (leftPart.startsWith("Range")) {
                String[] parts = leftPart.replace("Range(", "").replace(")", "").split(",");
                queryField = Integer.parseInt(parts[0].trim());
                String targetVectorFile = parts[1].trim() + ".txt";
                // System.out.println(targetVectorFile);
                int rangeThreshold = Integer.parseInt(parts[2].trim());
                Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);
                // System.out.println(targetVector);
                String useLSH = parts[3].trim();
                

                for (int i = 4; i < parts.length; i++) {
                    columns.add(Integer.parseInt(parts[i].trim()));
                }
                System.out.println(columns.indexOf(queryField));
                int noOutFlds = parts.length - 4;
                FldSpec[] projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer); 
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, Integer.parseInt(parts[4+i].trim()));
                }
                System.out.println(useLSH);
                RSIndexScan scan = null;
                if(useLSH.equals("H")){
                try{     
                        scan = new RSIndexScan(
                        new IndexType(IndexType.LSHF_Index), "data_heap.in",
                        relName1 + '_' + queryField + '_' + h + '_' + L,
                        attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, rangeThreshold
                    );
                }catch(Exception e){
                    e.printStackTrace();
                }
                
                // leftIter = new TupleListIterator(scan.get_all_results());

                }
                else{
                    try{     
                        scan = new RSIndexScan(
                        new IndexType(IndexType.None), "data_heap.in","",
                        attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, rangeThreshold
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
                        out_types[i] = attrType[projlist[i].offset - 1];
                        }
                    // result.print(out_types);
                    table.add(result.copy(out_types));
                    
                    
                }
                for(ArrayList<String> row:table){
                    System.out.println(row);
                }
                SystemDefs.JavabaseDB.closeDB();

            } else if (leftPart.startsWith("NN")) {
                String[] parts = leftPart.replace("NN(", "").replace(")", "").split(",");
                queryField = Integer.parseInt(parts[0].trim());
                String targetVectorFile = parts[1].trim() + ".txt";
                // System.out.println(targetVectorFile);
                int k = Integer.parseInt(parts[2].trim());
                Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);
                // System.out.println(targetVector);
                String useLSH = parts[3].trim();
                

                for (int i = 4; i < parts.length; i++) {
                    columns.add(Integer.parseInt(parts[i].trim()));
                }
                System.out.println(columns.indexOf(queryField));
                int noOutFlds = parts.length - 4;
                FldSpec[] projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer); 
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, Integer.parseInt(parts[4+i].trim()));
                }
                System.out.println(useLSH);
                NNIndexScan scan = null;
                if(useLSH.equals("H")){
                try{     
                        scan = new NNIndexScan(
                        new IndexType(IndexType.LSHF_Index), "data_heap.in","",
                        attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, k
                    );
                }catch(Exception e){
                    e.printStackTrace();
                }
                
                // leftIter = new TupleListIterator(scan.get_all_results());

                }
                else{
                    try{     
                        scan = new NNIndexScan(
                        new IndexType(IndexType.None), "data_heap.in",
                        relName1 + '_' + queryField + '_' + h + '_' + L,
                        attrType, getStringSizes(attrType), attrSize, noOutFlds,  projlist, null, queryField, targetVector, k
                    );
                    }catch(Exception e){
                        e.printStackTrace();
                }
                // leftIter = new TupleListIterator(scan.get_all_results());
                
            }
            List<Tuple> results = new ArrayList<>();
            results = scan.get_all_results();
            // System.out.println(results);
            AttrType[] out_types = new AttrType[noOutFlds];
            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = attrType[projlist[i].offset - 1];
                    }
                // result.print(out_types);
                table.add(result.copy(out_types));
                
                
            }
            // for(ArrayList<String> row:table){
            //     System.out.println(row);
            // }
            SystemDefs.JavabaseDB.closeDB();
        } else {
                throw new IllegalArgumentException("Unsupported DJOIN left query type.");
            }

            // FldSpec[] projList = new FldSpec[attrLen * 2];
            // for (int i = 0; i < attrLen; i++) {
            //     projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            //     projList[attrLen + i] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
            // }
            int num = numBuf;
            String dbpath2 = "/tmp/" + System.getProperty("user.name") + "."+relName2;
            INLJoins joinOP = new INLJoins(relName2,dbpath2,qf2,queryField, distanceThreshold2,num,useLSH2, table, rightParts, columns);
            ArrayList<ArrayList>result = joinOP.get_all_results();
            for (ArrayList<Object> tuple:result){
                System.out.println(tuple);
                System.out.println("\n");
            }
            // public INLJoins(String dbpath,int queryField, int distance, String useLSH, ArrayList<ArrayList> table, String parts){}

            // INLJoins joinOp = new INLJoins(
            //     attrType, attrLen, strSizes,
            //     attrType, attrLen, strSizes,
            //     100,
            //     leftIter, relName2,
            //     new IndexType(useLSHInner ? IndexType.LSHF_Index : IndexType.None),
            //     relName2 + "_" + joinFieldInner + "_" + h + "_" + L,
            //     joinFieldOuter, joinFieldInner,
            //     distanceThreshold, h, L, useLSHInner,
            //     projList, projList.length
            // );

            // Tuple t;
            // while ((t = joinOp.get_next()) != null) {
            //     t.print(attrType);
            // }

            // joinOp.close();

        } catch (Exception e) {
            e.printStackTrace();
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

    private static FldSpec[] buildProjection(int len) {
        FldSpec[] proj = new FldSpec[len];
        for (int i = 0; i < len; i++) {
            proj[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }
        return proj;
    }

    private static Vector100Dtype readVectorFromFile(String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String[] values = reader.readLine().trim().split("\\s+");
        reader.close();
        short[] vector = new short[100];
        for (int i = 0; i < 100; i++) {
            vector[i] = Short.parseShort(values[i]);
        }
        return new Vector100Dtype(vector);
    }

    public static void flushPages() {
        try {
            SystemDefs.JavabaseBM.flushAllPages(); // Assuming there's a method to flush all pages
            //System.out.println("All pages flushed to disk.");
        } catch (Exception e) {
            System.err.println("Error flushing pages: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
