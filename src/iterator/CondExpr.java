package iterator;

import java.io.*;
import global.*;

/**
 *  This class will hold a single select condition.
 *  It is an element of a linked list logically connected by OR operators.
 */
public class CondExpr {
  
  /**
   * Operator like "<", ">", "=" etc.
   */
  public AttrOperator op;    

  /**
   * Types of operands. Null AttrType means that operand is not a literal but an attribute name.
   */    
  public AttrType type1;
  public AttrType type2;    

  /**
   * The left operand and right operand 
   */ 
  public Operand operand1;
  public Operand operand2;

  /**
   * Pointer to the next element in the linked list
   */    
  public CondExpr next;   

  /**
   * Distance metric for Vector100Dtype comparisons.
   * Used only when both operands are of type attrVector100D.
   */
  public int distance; // Non-negative distance value

  /**
   * Constructor
   */
  public CondExpr() {
    operand1 = new Operand();
    operand2 = new Operand();
    
    operand1.integer = 0;
    operand2.integer = 0;
    
    distance = -1; // Default to -1 (invalid) until set in comparison logic
    next = null;
  }

  /**
   * Computes and updates the `distance` field if the operands are of type `attrVector100D`.
   */
  public void computeDistance() {
    if (type1 != null && type2 != null && 
        type1.attrType == AttrType.attrVector100D && 
        type2.attrType == AttrType.attrVector100D) {
      
      Vector100Dtype vector1 = operand1.vector100D;
      Vector100Dtype vector2 = operand2.vector100D;

      if (vector1 != null && vector2 != null) {
        // Ensure distance is non-negative and correctly rounded
        distance = (int) Math.round(Math.max(0, Vector100Dtype.computeDistance(vector1, vector2)));
      } else {
        System.err.println("Warning: One or both vector operands are null in computeDistance()");
        distance = -1; // Invalid state
      }
    }
  }
}
