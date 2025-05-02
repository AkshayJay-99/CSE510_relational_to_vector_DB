import diskmgr.PCounter;
import global.*;
import heap.*;
import index.*;
import iterator.*;

import java.io.*;
import java.util.*;

public class Query {

    public static void main(String relation1Name, String relation2Name, String queryFileName, int bufferPages) {

        // int    diskPages      = 1000;
        //String dbPath = "/tmp/" + System.getProperty("user.name") + "." + databaseName;

        System.out.println("Starting Query Execution...");
        System.out.println("Relation1: " + relation1Name);
        System.out.println("Relation2: " + relation2Name);
        System.out.println("Query File: " + queryFileName);
        System.out.println("Buffer Pages: " + bufferPages);

        try {
            // Initialize MiniBase
            //SystemDefs sysDef = new SystemDefs(dbPath, 0, bufferPages, "Clock");

            // Load schema from sc_heap.in
            FileScan schemaScan = new FileScan(
                    "sc_"+relation1Name+".in",
                    new AttrType[]{new AttrType(AttrType.attrString)},
                    new short[]{30},
                    (short) 1,
                    1,
                    new FldSpec[]{new FldSpec(new RelSpec(RelSpec.outer), 1)},
                    null
            );
            Tuple schemaTuple = schemaScan.get_next();
            schemaScan.close();
            if (schemaTuple == null) {
                System.out.println("Error: Schema not found");
                return;
            }

            String schemaString = schemaTuple.getStrFld(1);
            int numAttributes = schemaString.length() - 2;
            AttrType[] schema = new AttrType[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                int typeCode = schemaString.charAt(i) - '0';
                switch (typeCode) {
                    case 1:
                        schema[i] = new AttrType(AttrType.attrInteger);
                        break;
                    case 2:
                        schema[i] = new AttrType(AttrType.attrReal);
                        break;
                    case 3:
                        schema[i] = new AttrType(AttrType.attrString);
                        break;
                    case 4:
                        schema[i] = new AttrType(AttrType.attrVector100D);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown attribute type: " + typeCode);
                }
            }
            int hashFunctions = schemaString.charAt(schemaString.length() - 2) - '0';
            int indexLayers = schemaString.charAt(schemaString.length() - 1) - '0';

            // Read query specification
            BufferedReader reader = new BufferedReader(new FileReader(queryFileName));
            String queryLine = reader.readLine().trim();
            reader.close();

            // Dispatch to appropriate handler
            if (queryLine.startsWith("Sort("))
                processSortQuery(relation1Name, queryLine, schema, (short) numAttributes);
            else if (queryLine.startsWith("Filter("))
                processFilterQuery(relation1Name, queryLine, schema, (short) numAttributes);
            else if (queryLine.startsWith("Range("))
                processRangeQuery(relation1Name, queryLine, schema, (short) numAttributes, hashFunctions, indexLayers);
            else if (queryLine.startsWith("NN("))
                processNNQuery(relation1Name, queryLine, schema, (short) numAttributes, hashFunctions, indexLayers);
            else if (queryLine.startsWith("DJOIN(")) {
                if (queryLine.contains("Range("))
                    processDJoinRange(relation1Name, relation2Name, queryLine, schema, (short) numAttributes, hashFunctions, indexLayers);
                else
                    processDJoinNN(relation1Name, relation2Name, queryLine, schema, (short) numAttributes, hashFunctions, indexLayers);
            } else System.out.println("Invalid query type in file: " + queryFileName);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Flush buffers and report I/O
        flushPages();
        System.out.println("Disk pages read: " + PCounter.rcounter);
        System.out.println("Disk pages written: " + PCounter.wcounter);
    }


    // ---- Sort Query ----
    private static void processSortQuery(String relation1Name, String queryLine, AttrType[] schema, short numAttributes) {
        try {
            // 🔹 Extract parameters from "NN(QA, T, K, ...)"
            String[] parts = queryLine.replace("Sort(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim() + ".txt";
            int k = Integer.parseInt(parts[2].trim());
            // 🔹 Read the target vector from the file
            Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);

            FldSpec[] projlist = null;
            int noOutFlds = 0;
            if (parts.length == 4 && parts[3].trim().equals("*")) {
                noOutFlds = numAttributes;
                projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer);
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, i + 1);
                }
            } else {
                noOutFlds = parts.length - 3;
                projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer);
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, Integer.parseInt(parts[3 + i].trim()));
                }                
            }

            System.out.println("Processing Sort Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Number of Results: " + k);

            List<Tuple> results = new ArrayList<>();
            AttrType[] out_types = new AttrType[noOutFlds];
            NNIndexScan nn = null;

            System.out.println("Performing full heapfile scan for sorting...");
            TupleOrder[] order = new TupleOrder[2];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            order[1] = new TupleOrder(TupleOrder.Descending);
            try {
                nn = new NNIndexScan(new IndexType(IndexType.None), relation1Name+".in", "", schema, getStringSizes(schema), numAttributes, noOutFlds, projlist, null, queryField, targetVector, k);
            } catch (Exception e) {
                e.printStackTrace();
            }
            //  }
            results = nn.get_all_results();

            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = schema[projlist[i].offset - 1];
                }
                result.print(out_types);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ---- Filter Query ----
    private static void processFilterQuery(
            String relationName,
            String querySpecification,
            AttrType[] schema,
            short stringFieldSizes) {
        try {
            // 1. Extract everything inside the parentheses
            String inner = querySpecification
                    .substring(querySpecification.indexOf('(') + 1,
                            querySpecification.lastIndexOf(')'));
            String[] parts = inner.split(",");

            // 2. Parse the fixed parameters
            int    queryFieldIndex = Integer.parseInt(parts[0].trim());
            String literalValue    = parts[1].trim();
            // parts[2] is the unused K placeholder
            String indexOption     = parts[3].trim();

            // 3. Build the list of output‐fields
            List<Integer> outputFields = new ArrayList<>();
            if (parts.length == 5 && parts[4].trim().equals("*")) {
                outputFields.add(-1);
            } else {
                for (int i = 4; i < parts.length; i++) {
                    outputFields.add(Integer.parseInt(parts[i].trim()));
                }
            }

            // 4. Build the FldSpec[] and compute outCount
            FldSpec[] projSpecs;
            short     outCount;
            if (outputFields.size() == 1 && outputFields.get(0) == -1) {
                projSpecs = new FldSpec[schema.length];
                for (int i = 0; i < schema.length; i++) {
                    projSpecs[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                }
                outCount = (short) schema.length;
            } else {
                projSpecs = new FldSpec[outputFields.size()];
                for (int i = 0; i < outputFields.size(); i++) {
                    projSpecs[i] = new FldSpec(
                            new RelSpec(RelSpec.outer),
                            outputFields.get(i)
                    );
                }
                outCount = (short) outputFields.size();
            }

            // 5. Decide on index vs full scan
            if (indexOption.equalsIgnoreCase("H")) {
                System.out.println("Index option 'H' selected, but filter uses full scan only.");
            }

            // 6. Open FileScan on the heap file
            FileScan scan = new FileScan(
                     "data_heap.in",
                    schema,
                    getStringSizes(schema),
                    stringFieldSizes,
                    stringFieldSizes,
                    projSpecs,
                    null
            );

            // 7. Iterate and apply the filter predicate
            Tuple tuple;
            while ((tuple = scan.get_next()) != null) {
                boolean matches = false;
                AttrType at = schema[queryFieldIndex - 1];
                switch (at.attrType) {
                    case AttrType.attrInteger:
                        matches = (tuple.getIntFld(queryFieldIndex)
                                == Integer.parseInt(literalValue));
                        break;
                    case AttrType.attrReal:
                        matches = (tuple.getFloFld(queryFieldIndex)
                                == Float.parseFloat(literalValue));
                        break;
                    case AttrType.attrString:
                        matches = tuple.getStrFld(queryFieldIndex)
                                .equals(literalValue);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Cannot filter on attribute type: " + at.attrType);
                }
                if (matches) {
                    printTupleProjection(tuple, schema, outputFields);
                }
            }
            scan.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }





    // ---- Range Query ----
   /* private static void processRangeQuery(
            String relName, String queryLine,
            AttrType[] schema, short attrSize, int h, int L) {
        try {
            String inside = queryLine.substring(queryLine.indexOf('(')+1, queryLine.lastIndexOf(')'));
            String[] p = inside.split(",");
            int qf          = Integer.parseInt(p[0].trim());
            String tgtFile  = p[1].trim()+".txt";
            int thr         = Integer.parseInt(p[2].trim());
            String idxOpt   = p[3].trim();
            List<Integer> pf= new ArrayList<>();
            if (p.length==5 && p[4].trim().equals("*")) pf.add(-1);
            else for(int i=4;i<p.length;i++) pf.add(Integer.parseInt(p[i].trim()));

            Vector100Dtype tgt = readVectorFromFile(tgtFile);
            RSIndexScan rs;
            if (idxOpt.equalsIgnoreCase("H")) {
                rs = new RSIndexScan(
                        new IndexType(IndexType.LSHF_Index),
                        relName + "_heap.in",   // data file
                        dbName + "_"+qf+"_"+h+"_"+L, // index name
                        schema, getStringSizes(schema), attrSize,
                        pf.size(), buildProjList(pf), null,
                        qf, tgt, thr
                );
            } else {
                rs = new RSIndexScan(
                        new IndexType(IndexType.None),
                        relName + "_heap.in", "",
                        schema, getStringSizes(schema), attrSize,
                        pf.size(), buildProjList(pf), null,
                        qf, tgt, thr
                );
            }
            List<Tuple> res = rs.get_all_results();
            for (Tuple t : res) t.print(schema);
        } catch(Exception e){ e.printStackTrace(); }
    }*//*  try {
            // Extract the comma-separated parameters inside the parentheses
            String params = querySpecification
                    .substring(querySpecification.indexOf('(') + 1,
                            querySpecification.lastIndexOf(')'));
            String[] tokens = params.split(",");

            // Parse each piece
            int queryFieldIndex = Integer.parseInt(tokens[0].trim());
            String targetVectorFile = tokens[1].trim() + ".txt";
            int distanceThreshold = Integer.parseInt(tokens[2].trim());
            String indexOption = tokens[3].trim();

            // Build projection list
            List<Integer> projectionFields = new ArrayList<>();
            if (tokens.length == 5 && tokens[4].trim().equals("*")) {
                projectionFields.add(-1);
            } else {
                for (int i = 4; i < tokens.length; i++) {
                    projectionFields.add(Integer.parseInt(tokens[i].trim()));
                }
            }

            // Read target vector
            Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);

            // Determine data and index filenames
            String dataFileName = relationName + "_heap.in";
            String indexFileName = "";
            IndexType idxType = new IndexType(IndexType.None);

            if (indexOption.equalsIgnoreCase("H")) {
                idxType = new IndexType(IndexType.LSHF_Index);
                indexFileName = databaseName + "_"
                        + queryFieldIndex + "_"
                        + numHashFunctions + "_"
                        + numIndexLayers;
            }

            // Create the range scan
            RSIndexScan rangeScan = new RSIndexScan(idxType, dataFileName, indexFileName, schema,
                    getStringSizes(schema), tupleStrSizes,
                    projectionFields.size(), buildProjList(projectionFields),
                    null, queryFieldIndex, targetVector, distanceThreshold
            );

            // Fetch and print all matching tuples
            List<Tuple> results = rangeScan.get_all_results();
            for (Tuple tuple : results) {
                tuple.print(schema);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }*/
    private static void processRangeQuery(String relationName, String querySpecification, AttrType[] schema, short numAttributes, int h, int L) {
        try {
            // 🔹 Extract parameters from "Range(QA, T, D, ...)"

            String[] parts = querySpecification.replace("Range(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim()+".txt";
            int distanceThreshold = Integer.parseInt(parts[2].trim());
            String useIndexOption = parts[3].trim();
            // 🔹 Read the target vector from the file
            Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);
            int noOutFlds = 0;
            FldSpec[] projlist = null;

            if (parts.length == 5 && parts[4].trim().equals("*")) {
                noOutFlds = numAttributes;
                projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer);
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, i + 1);
                }
            } else {
                noOutFlds = parts.length - 4;
                projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer);
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, Integer.parseInt(parts[4 + i].trim()));
                }                
            }
            System.out.println("Processing Range Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Distance Threshold: " + distanceThreshold);


            List<Tuple> results = new ArrayList<>();
            AttrType[] out_types = new AttrType[noOutFlds];
            RSIndexScan rs = null;
            if (useIndexOption.equalsIgnoreCase("H")) {
                System.out.println("Using LSH-Forest for range query...");
                try {
                    rs = new RSIndexScan(new IndexType(IndexType.LSHF_Index), relationName+".in", "databaseName"+'_'+queryField+'_'+h+'_'+L, schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, distanceThreshold);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                System.out.println("Performing full heapfile scan for range query...");
                TupleOrder[] order = new TupleOrder[2];
                order[0] = new TupleOrder(TupleOrder.Ascending);
                order[1] = new TupleOrder(TupleOrder.Descending);
                try {
                    rs = new RSIndexScan(new IndexType(IndexType.None), "data_heap.in", "", schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, distanceThreshold);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            results = rs.get_all_results();

            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = schema[projlist[i].offset - 1];
                }
                result.print(out_types);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // ---- NN Query ----
    private static void processNNQuery( String relName, String queryLine, AttrType[] schema, short numAttributes, int h, int L){
        try {
            // 🔹 Extract parameters from "NN(QA, T, K, ...)"
            String[] parts = queryLine.replace("NN(", "").replace(")", "").split(",");
            int queryField = Integer.parseInt(parts[0].trim());
            String targetVectorFile = parts[1].trim()+".txt";
            int k = Integer.parseInt(parts[2].trim());
            String useIndexOption = parts[3].trim();
            // 🔹 Read the target vector from the file
            Vector100Dtype targetVector = readVectorFromFile(targetVectorFile);

            int noOutFlds = 0;
            FldSpec[] projlist = null;
            if (parts.length == 5 && parts[4].trim().equals("*")) {
                noOutFlds = numAttributes;
                projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer);
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, i + 1);
                }
            } else {
                noOutFlds = parts.length - 4;
                projlist = new FldSpec[noOutFlds];
                RelSpec rel = new RelSpec(RelSpec.outer);
                for (int i = 0; i < noOutFlds; i++) {
                    projlist[i] = new FldSpec(rel, Integer.parseInt(parts[4 + i].trim()));
                }                
            }
            System.out.println("Processing Nearest Neighbor Query...");
            System.out.println("Query Field: " + queryField);
            System.out.println("Target Vector File: " + targetVectorFile);
            System.out.println("Number of Neighbors: " + k);

            List<Tuple> results = new ArrayList<>();
            AttrType[] out_types = new AttrType[noOutFlds];
            NNIndexScan nn = null;
            if (useIndexOption.equalsIgnoreCase("H")) {
                System.out.println("Using LSH-Forest for nearest neighbor search...");
                try {
                    nn = new NNIndexScan(new IndexType(IndexType.LSHF_Index),  relName+".in", relName+'_'+queryField+'_'+h+'_'+L, schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, k);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                System.out.println("Performing full heapfile scan for nearest neighbors...");
                System.out.println("Performing full heapfile scan for range query...");
                TupleOrder[] order = new TupleOrder[2];
                order[0] = new TupleOrder(TupleOrder.Ascending);
                order[1] = new TupleOrder(TupleOrder.Descending);
                try {
                    nn = new NNIndexScan(new IndexType(IndexType.None), relName+".in", "", schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, k);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            results = nn.get_all_results();

            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = schema[projlist[i].offset - 1];
                }
                result.print(out_types);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    // ---- Distance Join (Range inner) ----
    private static void processDJoinRange(
            String rel1, String rel2, String queryLine,
            AttrType[] schema, short attrSize, int h, int L) {
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---- Distance Join (NN inner) ----
    private static void processDJoinNN(
            String rel1, String rel2, String queryLine,
            AttrType[] schema, short attrSize, int h, int L) {
        try {


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---- Helpers ----

    private static void printTupleProjection(
            Tuple tuple,
            AttrType[] schema,
            List<Integer> projFields
    ) throws Exception {
        // If projFields == [-1], print the entire tuple:
        if (projFields.size() == 1 && projFields.get(0) == -1) {
            tuple.print(schema);
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < projFields.size(); i++) {
            int fldNo = projFields.get(i);            // 1-based field number
            AttrType at = schema[fldNo - 1];          // AttrType for that column
            String val;
            switch (at.attrType) {
                case AttrType.attrInteger:
                    val = Integer.toString(tuple.getIntFld(fldNo));
                    break;
                case AttrType.attrReal:
                    val = Float.toString(tuple.getFloFld(fldNo));
                    break;
                case AttrType.attrString:
                    val = tuple.getStrFld(fldNo);
                    break;
                case AttrType.attrVector100D:
                    Vector100Dtype vec = tuple.get100DVectorFld(fldNo);
                    val = Arrays.toString(vec.getValues());
                    break;
                default:
                    throw new RuntimeException("Unknown attrType: " + at.attrType);
            }
            sb.append(val);
            if (i < projFields.size() - 1) sb.append(" | ");
        }
        System.out.println(sb.toString());
    }



    private static double computeDistance(short[] v1, short[] v2) {
        double sum = 0;
        for (int i = 0; i < v1.length; i++) {
            double d = v1[i] - v2[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }

    private static Vector100Dtype readVectorFromFile(String fn) {
        try {
            BufferedReader r = new BufferedReader(new FileReader(fn));
            String[] tok = r.readLine().trim().split("\\s+");
            r.close();
            if (tok.length != 100) throw new IllegalArgumentException("Need 100 dims");
            short[] vec = new short[100];
            for (int i = 0; i < 100; i++) vec[i] = Short.parseShort(tok[i]);
            return new Vector100Dtype(vec);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static short[] getStringSizes(AttrType[] schema) {
        int cnt = 0;
        for (AttrType a : schema) if (a.attrType == AttrType.attrString) cnt++;
        short[] sz = new short[cnt];
        Arrays.fill(sz, (short) 30);
        return sz;
    }

    private static FldSpec[] buildProjList(List<Integer> pf) {
        if (pf.size() == 1 && pf.get(0) == -1) return null;
        FldSpec[] ps = new FldSpec[pf.size()];
        for (int i = 0; i < pf.size(); i++) ps[i] = new FldSpec(new RelSpec(RelSpec.outer), pf.get(i));
        return ps;
    }


    private static void flushPages() {
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            System.out.println("All pages flushed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class TupleDistance {
        Tuple tuple;
        double distance;

        TupleDistance(Tuple t, double d) {
            tuple = t;
            distance = d;
        }
    }
}
