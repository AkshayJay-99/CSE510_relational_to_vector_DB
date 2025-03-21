package iterator;

import heap.*;
import global.*;
import java.io.*;

public class PredEval {

  /**
   * Evaluates condition expressions.
   * If attributes are of type `attrVector100D`, the comparison is based on Euclidean distance.
   *
   * @param p[] single select condition array
   * @param t1 compared tuple1
   * @param t2 compared tuple2
   * @param in1[] the attribute type corresponding to t1
   * @param in2[] the attribute type corresponding to t2
   * @return true or false
   * @throws IOException, UnknowAttrType, InvalidTupleSizeException,
   *         InvalidTypeException, FieldNumberOutOfBoundException, PredEvalException
   */
  public static boolean Eval(CondExpr p[], Tuple t1, Tuple t2, AttrType in1[], AttrType in2[])
      throws IOException, UnknowAttrType, InvalidTupleSizeException, InvalidTypeException,
             FieldNumberOutOfBoundException, PredEvalException {
    
    CondExpr temp_ptr;
    int i = 0;
    Tuple tuple1 = null, tuple2 = null;
    int fld1, fld2;
    Tuple value = new Tuple();
    short[] str_size = new short[1];
    AttrType[] val_type = new AttrType[1];

    AttrType comparison_type = new AttrType(AttrType.attrInteger);
    int comp_res;
    boolean op_res = false, row_res = false, col_res = true;

    if (p == null) {
      return true;
    }

    while (p[i] != null) {
      temp_ptr = p[i];
      while (temp_ptr != null) {
        val_type[0] = new AttrType(temp_ptr.type1.attrType);
        fld1 = 1;

        // Handling for different attribute types
        switch (temp_ptr.type1.attrType) {
          case AttrType.attrInteger:
            value.setHdr((short) 1, val_type, null);
            value.setIntFld(1, temp_ptr.operand1.integer);
            tuple1 = value;
            comparison_type.attrType = AttrType.attrInteger;
            break;
          case AttrType.attrReal:
            value.setHdr((short) 1, val_type, null);
            value.setFloFld(1, temp_ptr.operand1.real);
            tuple1 = value;
            comparison_type.attrType = AttrType.attrReal;
            break;
          case AttrType.attrString:
            str_size[0] = (short) (temp_ptr.operand1.string.length() + 1);
            value.setHdr((short) 1, val_type, str_size);
            value.setStrFld(1, temp_ptr.operand1.string);
            tuple1 = value;
            comparison_type.attrType = AttrType.attrString;
            break;
          case AttrType.attrSymbol:
            fld1 = temp_ptr.operand1.symbol.offset;
            if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer) {
              tuple1 = t1;
              comparison_type.attrType = in1[fld1 - 1].attrType;
            } else {
              tuple1 = t2;
              comparison_type.attrType = in2[fld1 - 1].attrType;
            }
            break;
          default:
            break;
        }

        // Handling for second argument in comparison
        val_type[0] = new AttrType(temp_ptr.type2.attrType);
        fld2 = 1;

        switch (temp_ptr.type2.attrType) {
          case AttrType.attrInteger:
            value.setHdr((short) 1, val_type, null);
            value.setIntFld(1, temp_ptr.operand2.integer);
            tuple2 = value;
            break;
          case AttrType.attrReal:
            value.setHdr((short) 1, val_type, null);
            value.setFloFld(1, temp_ptr.operand2.real);
            tuple2 = value;
            break;
          case AttrType.attrString:
            str_size[0] = (short) (temp_ptr.operand2.string.length() + 1);
            value.setHdr((short) 1, val_type, str_size);
            value.setStrFld(1, temp_ptr.operand2.string);
            tuple2 = value;
            break;
          case AttrType.attrSymbol:
            fld2 = temp_ptr.operand2.symbol.offset;
            if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer) {
              tuple2 = t1;
            } else {
              tuple2 = t2;
            }
            break;
          default:
            break;
        }

        // Special handling for attrVector100D
        if (comparison_type.attrType == AttrType.attrVector100D) {
          Vector100Dtype vector1 = t1.get100DVectorFld(fld1);
          Vector100Dtype vector2 = t2.get100DVectorFld(fld2);
          
          if (vector1 != null && vector2 != null) {
            temp_ptr.distance = (int) Math.max(0, Vector100Dtype.computeDistance(vector1, vector2));
          } else {
            temp_ptr.distance = -1; // Invalid comparison
          }
        }

        // Perform the comparison
        try {
          if (comparison_type.attrType == AttrType.attrVector100D) {
            comp_res = temp_ptr.distance; // Use computed distance
          } else {
            comp_res = TupleUtils.CompareTupleWithTuple(comparison_type, tuple1, fld1, tuple2, fld2);
          }
        } catch (TupleUtilsException e) {
          throw new PredEvalException(e, "TupleUtilsException caught by PredEval.java");
        }

        op_res = false;

        // Handling comparison operators for attrVector100D
        switch (temp_ptr.op.attrOperator) {
          case AttrOperator.aopEQ:
            op_res = (comp_res == temp_ptr.distance);
            break;
          case AttrOperator.aopLT:
            op_res = (comp_res < temp_ptr.distance);
            break;
          case AttrOperator.aopGT:
            op_res = (comp_res > temp_ptr.distance);
            break;
          case AttrOperator.aopNE:
            op_res = (comp_res != temp_ptr.distance);
            break;
          case AttrOperator.aopLE:
            op_res = (comp_res <= temp_ptr.distance);
            break;
          case AttrOperator.aopGE:
            op_res = (comp_res >= temp_ptr.distance);
            break;
          case AttrOperator.aopNOT:
            op_res = (comp_res != 0);
            break;
          default:
            break;
        }

        row_res = row_res || op_res;
        if (row_res) break; // OR predicates satisfied.
        temp_ptr = temp_ptr.next;
      }
      i++;

      col_res = col_res && row_res;
      if (!col_res) {
        return false;
      }
      row_res = false; // Reset for the next row.
    }

    return true;
  }
}
