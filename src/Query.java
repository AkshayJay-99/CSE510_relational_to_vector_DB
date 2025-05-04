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
            Heapfile heapfile = new Heapfile("sc_"+relation1Name+".in");
            Scan scan = heapfile.openScan();
            Tuple tuple;
            RID rid = new RID();
            tuple = scan.getNext(rid);
            tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
            tuple.print(new AttrType[]{new AttrType(AttrType.attrString)});
    
            String attributes = tuple.getStrFld(1);
            char[] attr = attributes.toCharArray();
            AttrType[] schema = new AttrType[attr.length];
            int numAttributes = attributes.length();

    
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
    
            scan.closescan();
            
            int hashFunctions = 0;
            int indexLayers = 0;

        
            // Read query specification
            String queryLine;
            BufferedReader reader = new BufferedReader(new FileReader(queryFileName));
            StringBuilder content = new StringBuilder();
            while ((queryLine = reader.readLine()) != null) {
                content.append(queryLine.trim());  // or just queryLine if you don't want trimming
            }
            reader.close();
            
            // Now you have all content in one String
            queryLine = content.toString();
            PCounter.rcounter = 0;
            PCounter.wcounter = 0;
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
            //System.out.println("Number of Results: " + k);

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
            int resultLength = results.size();
            System.out.println("Number of Results: " + resultLength);

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


    private static void processFilterQuery(
        String relationName,
        String querySpecification,
        AttrType[] schema,
        short numAttributes) {
    try {
        // 1. Parse “Filter(QA, T, K, I, …)”
        String[] parts = querySpecification.replace("Filter(", "").replace(")", "").split(",");
        int queryFieldIndex = Integer.parseInt(parts[0].trim());
        int literalValue = Integer.parseInt(parts[1].trim());
        //int k = Integer.parseInt(parts[2].trim());
        String indexOption = parts[2].trim();

        System.out.println("Processing Filter Query...");
        System.out.println("Query Field: " + queryFieldIndex);
        System.out.println("Target Value: " + literalValue);
        System.out.println("Index Option: " + indexOption);
        String dataFile  = relationName + ".in";
        String indexFile = relationName + "_" + queryFieldIndex;
        
        if (SystemDefs.JavabaseDB.get_file_entry(indexFile)== null && indexOption.equalsIgnoreCase("H")) {
            System.out.println("Index file not found for " + queryFieldIndex + ". Please create the index first.");
            return;
        }

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


        int outFieldIndex = -1;
        AttrType[] out_types = new AttrType[noOutFlds];
        for (int i = 0; i < noOutFlds; i++) {
            out_types[i] = schema[projlist[i].offset - 1];
            if (projlist[i].offset - 1 == queryFieldIndex-1) {
                outFieldIndex = i+1;
            }
        }

        System.out.println("Index File: " + indexFile);
        System.out.println("Data File: " + dataFile);

        if (indexOption.equalsIgnoreCase("H")) {
            System.out.println("Using B+Tree index on field " + queryFieldIndex);

            CondExpr[] filterExpr = new CondExpr[3]; 
            filterExpr[0] = new CondExpr();
            filterExpr[0].op = new AttrOperator(AttrOperator.aopGE);
            filterExpr[0].type1 = new AttrType(AttrType.attrSymbol);
            filterExpr[0].type2 = new AttrType(AttrType.attrInteger);
            filterExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryFieldIndex);
            filterExpr[0].operand2.integer = literalValue;
            filterExpr[1] = new CondExpr();
            filterExpr[1].op = new AttrOperator(AttrOperator.aopLE);
            filterExpr[1].type2 = new AttrType(AttrType.attrSymbol);
            filterExpr[1].type1 = new AttrType(AttrType.attrInteger);
            filterExpr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryFieldIndex);
            filterExpr[1].operand1.integer = literalValue;
            filterExpr[2] = null;

            IndexScan iscan = null;
            try {
              iscan = new IndexScan(new IndexType(IndexType.B_Index), dataFile, indexFile, schema, getStringSizes(schema), numAttributes, noOutFlds, projlist, filterExpr, queryFieldIndex, false);
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            Tuple t = null;
            try {
                t = iscan.get_next();
              }
              catch (Exception e) {
                e.printStackTrace(); 
              }
            while (t != null) {
                try {
                    t.print(out_types);
                    t = iscan.get_next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
            }
            try {
                iscan.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            flushPages();
        } else {
            System.out.println("Performing full heapfile scan");

            CondExpr[] filterExpr = new CondExpr[2]; 
            filterExpr[0] = new CondExpr();
            filterExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
            filterExpr[0].type1 = new AttrType(AttrType.attrSymbol);
            filterExpr[0].type2 = new AttrType(AttrType.attrInteger);
            filterExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryFieldIndex);
            filterExpr[0].operand2.integer = literalValue;
            filterExpr[1] = null;

            FileScan fscan = null;
            try {
                fscan = new FileScan(dataFile, schema, getStringSizes(schema), (short) numAttributes, noOutFlds, projlist, null);
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            Tuple t = null;
            try {
                t = fscan.get_next();
            }
              catch (Exception e) {
                e.printStackTrace(); 
              }
            while (t != null) {
                try {
                    if (t.getFloFld(outFieldIndex) == literalValue) {
                        t.print(out_types);
                    }
                    t = fscan.get_next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
            }
            
            fscan.close();
            flushPages();
        }

    } catch (Exception e) {
        e.printStackTrace();
    }
}

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

            Heapfile index_heapfile = new Heapfile(relationName + "indexes");
            Scan scan = index_heapfile.openScan();
            //tuple;
            Tuple tuple;
            RID rid = new RID();
            boolean found = false;
            while((tuple = scan.getNext(rid)) != null)
            {
                tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                String index_String = tuple.getStrFld(1);
                String[] index_parts = index_String.split("_");
                if (queryField == Integer.parseInt(index_parts[1])) {
                    h = Integer.parseInt(index_parts[3]);
                    L = Integer.parseInt(index_parts[2]);    
                    System.out.println("Index file found: " + index_String);
                    found = true;
                }
            }

            if (!found && useIndexOption.equalsIgnoreCase("H")) {
                System.out.println("Index file not found for " + queryField + ". Please create the index first.");
                return;
            }
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
            System.out.println("Range: " + distanceThreshold);

            List<Tuple> results = new ArrayList<>();
            AttrType[] out_types = new AttrType[noOutFlds];
            RSIndexScan rs = null;
            if (useIndexOption.equalsIgnoreCase("H")) {
                System.out.println("Using LSH-Forest for range query...");
                try {
                    rs = new RSIndexScan(new IndexType(IndexType.LSHF_Index), relationName+".in", relationName+ "_" + queryField+'_'+L+'_'+h, schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, distanceThreshold);
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
                    rs = new RSIndexScan(new IndexType(IndexType.None), relationName+".in", "", schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, distanceThreshold);
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

            Heapfile index_heapfile = new Heapfile(relName + "indexes");
            Scan scan = index_heapfile.openScan();
            //tuple;
            Tuple tuple;
            RID rid = new RID();
            boolean found = false;
            while((tuple = scan.getNext(rid)) != null)
            {
                tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                String index_String = tuple.getStrFld(1);
                String[] index_parts = index_String.split("_");
                if (queryField == Integer.parseInt(index_parts[1])) {
                    h = Integer.parseInt(index_parts[3]);
                    L = Integer.parseInt(index_parts[2]);           
                    System.out.println("Index file found: " + index_String);
                    found = true;
                }
            }

            if (!found && useIndexOption.equalsIgnoreCase("H")) {
                System.out.println("Index file not found for " + queryField + ". Please create the index first.");
                return;
            }

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
                    nn = new NNIndexScan(new IndexType(IndexType.LSHF_Index),  relName+".in", relName + "_" + queryField + "_" + L + "_"+ h, schema, getStringSizes(schema), numAttributes, noOutFlds,  projlist, null, queryField, targetVector, k);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                System.out.println("Performing full heapfile scan for nearest neighbors...");
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
            String inside = queryLine.replace("DJOIN(", "");
            // System.out.println(inside);
            int firstClose = inside.indexOf(")");
            String leftPart = inside.substring(0, firstClose + 1);
            String[] rightParts = inside.substring(firstClose + 2).replace(")","").split(",");
            int qf2 = Integer.parseInt(rightParts[0]);
            int distanceThreshold2 = Integer.parseInt(rightParts[1].trim());
            String useLSH2 = rightParts[2].trim();
            ArrayList<Integer> columns2 = new ArrayList<>();
            ArrayList<Object> columns = new ArrayList<>();

            for (int i = 3; i < rightParts.length; i++) {
                columns2.add(Integer.parseInt(rightParts[i].trim()));
            }
            // System.out.println(rightParts);
            
    
            int distanceThreshold = Integer.parseInt(rightParts[1].trim());
            String innerIndexOption = rightParts[2].trim();
            boolean useLSHInner = innerIndexOption.equalsIgnoreCase("H");
            int queryField;
            ArrayList<ArrayList> table = new ArrayList<>();
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
            Heapfile index_heapfile = new Heapfile(rel1 + "indexes");
            Scan scan = index_heapfile.openScan();
            //tuple;
            Tuple tuple;
            boolean found = false;
            RID rid = new RID();
            while((tuple = scan.getNext(rid)) != null)
            {
                tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                String index_String = tuple.getStrFld(1);
                String[] index_parts = index_String.split("_");
                if (queryField == Integer.parseInt(index_parts[1])) {
                    h = Integer.parseInt(index_parts[3]);
                    L = Integer.parseInt(index_parts[2]);           
                    found = true;
                }
            }

            if (!found && useLSH.equalsIgnoreCase("H")) {
                System.out.println(rel1+"Index file not found for " + queryField + ". Please create the index first.");
                return;
            }

            Heapfile rel2_index_heapfile = new Heapfile(rel2 + "indexes");
            Scan rel2_scan = rel2_index_heapfile.openScan();
            //tuple;
            Tuple rel2_tuple;
            boolean rel2_found = false;
            RID rel2_rid = new RID();
            while((rel2_tuple = rel2_scan.getNext(rel2_rid)) != null)
            {
                rel2_tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                String index_String = rel2_tuple.getStrFld(1);
                String[] index_parts = index_String.split("_");
                if (qf2 == Integer.parseInt(index_parts[1])) {        
                    rel2_found = true;
                }
            }

            if (!rel2_found && useLSH2.equalsIgnoreCase("H")) {
                System.out.println(rel2+"Index file not found for " + qf2 + ". Please create the index first.");
                return;
            }

            int noOutFlds = 0;
            FldSpec[] projlist = null;
            if (parts.length == 5 && parts[4].trim().equals("*")) {
                noOutFlds = attrSize;
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
            AttrType[] out_types = new AttrType[noOutFlds];
            RSIndexScan rs = null;
            if (useLSH.equalsIgnoreCase("H")) {
                System.out.println("Using LSH-Forest for range query...");
                try {
                    rs = new RSIndexScan(new IndexType(IndexType.LSHF_Index), rel1+".in", rel1+ "_" + queryField+'_'+L+'_'+h, schema, getStringSizes(schema), attrSize, noOutFlds,  projlist, null, queryField, targetVector, rangeThreshold);
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
                    rs = new RSIndexScan(new IndexType(IndexType.None), rel1+".in", "", schema, getStringSizes(schema), attrSize, noOutFlds,  projlist, null, queryField, targetVector, rangeThreshold);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            List<Tuple> results = new ArrayList<>();
            results = rs.get_all_results();
            // System.out.println(results);
            for (Tuple result : results) {
                for (int i = 0; i < noOutFlds; i++) {
                    out_types[i] = schema[projlist[i].offset - 1];
                    }
                result.print(out_types);
                table.add(result.copy(out_types));
                }
                INLJoins joinOP = new INLJoins(rel2,qf2,queryField, distanceThreshold2,useLSH2, table, rightParts, columns);
                ArrayList<ArrayList>join_result = joinOP.get_all_results();
                for (ArrayList<Object> tuples:join_result){
                    System.out.println(tuples);
                    System.out.println("\n");
                }
        
     }catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---- Distance Join (NN inner) ----
    private static void processDJoinNN(
            String rel1, String rel2, String queryLine,
            AttrType[] schema, short attrSize, int h, int L) {
                try {
                    String inside = queryLine.replace("DJOIN(", "");
                    // System.out.println(inside);
                    int firstClose = inside.indexOf(")");
                    String leftPart = inside.substring(0, firstClose + 1);
                    String[] rightParts = inside.substring(firstClose + 2).replace(")","").split(",");
                    int qf2 = Integer.parseInt(rightParts[0]);
                    int distanceThreshold2 = Integer.parseInt(rightParts[1].trim());
                    String useLSH2 = rightParts[2].trim();
                    ArrayList<Integer> columns2 = new ArrayList<>();
                    ArrayList<Object> columns = new ArrayList<>();
        
                    for (int i = 3; i < rightParts.length; i++) {
                        columns2.add(Integer.parseInt(rightParts[i].trim()));
                    }
                    // System.out.println(rightParts);
                    
            
                    int K = Integer.parseInt(rightParts[1].trim());
                    String innerIndexOption = rightParts[2].trim();
                    boolean useLSHInner = innerIndexOption.equalsIgnoreCase("H");
                    int queryField;
                    ArrayList<ArrayList> table = new ArrayList<>();
                    String[] parts = leftPart.replace("NN(", "").replace(")", "").split(",");
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
                    Heapfile index_heapfile = new Heapfile(rel1 + "indexes");
                    Scan scan = index_heapfile.openScan();
                    //tuple;
                    Tuple tuple;
                    RID rid = new RID();
                    boolean found = false;
                    while((tuple = scan.getNext(rid)) != null)
                    {
                        tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                        String index_String = tuple.getStrFld(1);
                        String[] index_parts = index_String.split("_");
                        if (queryField == Integer.parseInt(index_parts[1])) {
                            h = Integer.parseInt(index_parts[3]);
                            L = Integer.parseInt(index_parts[2]);  
                            found = true;
                        }
                    }
        
                    if (!found && useLSH.equalsIgnoreCase("H")) {
                        System.out.println(rel1+"Index file not found for " + queryField + ". Please create the index first.");
                        return;
                    }
        
                    Heapfile rel2_index_heapfile = new Heapfile(rel2 + "indexes");
                    Scan rel2_scan = rel2_index_heapfile.openScan();
                    //tuple;
                    Tuple rel2_tuple;
                    boolean rel2_found = false;
                    RID rel2_rid = new RID();
                    while((rel2_tuple = rel2_scan.getNext(rel2_rid)) != null)
                    {
                        rel2_tuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrString)}, new short[]{30});
                        String index_String = rel2_tuple.getStrFld(1);
                        String[] index_parts = index_String.split("_");
                        if (qf2 == Integer.parseInt(index_parts[1])) {        
                            rel2_found = true;
                        }
                    }
        
                    if (!rel2_found && useLSH2.equalsIgnoreCase("H")) {
                        System.out.println(rel2+"Index file not found for " + qf2 + ". Please create the index first.");
                        return;
                    }
        
                    int noOutFlds = 0;
                    FldSpec[] projlist = null;
                    if (parts.length == 5 && parts[4].trim().equals("*")) {
                        noOutFlds = attrSize;
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
                    AttrType[] out_types = new AttrType[noOutFlds];
                    NNIndexScan nn = null;
                    if (useLSH.equalsIgnoreCase("H")) {
                        System.out.println("Using LSH-Forest for range query...");
                        try {
                            nn = new NNIndexScan(new IndexType(IndexType.LSHF_Index), rel1+".in", rel1+ "_" + queryField+'_'+L+'_'+h, schema, getStringSizes(schema), attrSize, noOutFlds,  projlist, null, queryField, targetVector, K);
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
                            nn = new NNIndexScan(new IndexType(IndexType.None), rel1+".in", "", schema, getStringSizes(schema), attrSize, noOutFlds,  projlist, null, queryField, targetVector, K);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    List<Tuple> results = new ArrayList<>();
                    results = nn.get_all_results();
                    // System.out.println(results);
                    for (Tuple result : results) {
                        for (int i = 0; i < noOutFlds; i++) {
                            out_types[i] = schema[projlist[i].offset - 1];
                            }
                        result.print(out_types);
                        table.add(result.copy(out_types));
                        }
                        INLJoins joinOP = new INLJoins(rel2,qf2,queryField, distanceThreshold2,useLSH2, table, rightParts, columns);
                        ArrayList<ArrayList>join_result = joinOP.get_all_results();
                        for (ArrayList<Object> tuples:join_result){
                            System.out.println(tuples);
                            System.out.println("\n");
                }
             }catch (Exception e) {
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
